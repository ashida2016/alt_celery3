#!/usr/bin/env python
"""生产者端示例脚本。

调用本项目中的全部示例任务并获取执行结果：

1. 普通任务 ``tasks.add``：异步下发并阻塞等待结果。
2. 定时任务 ``tasks.periodic_add``：演示手动下发同一任务并等待结果
   （该任务同时由 celery beat 周期调度）；并从 result backend
   （redis）中查询最近的历史执行结果，其中包含 beat 触发的定时执行。

用法::

    # 连接参数自动从项目根目录的 .env 文件加载，
    # 修改 .env 中的 CELERY_BROKER_URL / CELERY_RESULT_BACKEND 即可。
    python run_tasks.py
    python run_tasks.py --timeout 10
"""

import argparse
import json
import logging
import sys
from typing import Any

from app import app
from app.tasks.db_tasks import (
    generate_many_students,
    get_one_student,
    try_mysql,
)
from app.tasks.math_tasks import add, periodic_add
from app.tasks.un_tasks import get_un_groups

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("run_tasks")


def run_normal_task(x: int, y: int, timeout: float) -> int:
    """下发普通加法任务并阻塞等待结果。

    Args:
        x: 加数。
        y: 被加数。
        timeout: 等待结果的最长秒数，超时抛出异常。

    Returns:
        加法任务的返回值。

    Raises:
        TimeoutError: 结果等待超时。
    """
    logger.info("下发普通任务 tasks.add(%d, %d) ...", x, y)
    async_result = add.delay(x, y)
    result = async_result.get(timeout=timeout)
    logger.info("tasks.add 结果: task_id=%s, result=%s", async_result.id, result)
    return result


def run_scheduled_task(x: int, y: int, timeout: float) -> int:
    """下发定时任务对应的任务函数并等待结果。

    ``tasks.periodic_add`` 同时被 celery beat 周期调度，此处演示
    以生产者身份调用同一任务（功能上等价于 beat 触发一次执行）。

    Args:
        x: 加数。
        y: 被加数。
        timeout: 等待结果的最长秒数。

    Returns:
        定时任务的返回值。
    """
    logger.info("下发定时任务 tasks.periodic_add(%d, %d) ...", x, y)
    async_result = periodic_add.delay(x, y)
    result = async_result.get(timeout=timeout)
    logger.info(
        "tasks.periodic_add 结果: task_id=%s, result=%s",
        async_result.id,
        result,
    )
    return result


def list_recent_task_results(limit: int = 10) -> list[dict[str, Any]]:
    """从 result backend（redis）查询最近的任务执行结果。

    celery 会把每个任务的执行结果以 ``celery-task-meta-<task_id>`` 为
    键写入 redis backend，其中也包含由 beat 周期触发的定时任务
    （tasks.periodic_add）的历史结果。本函数按写入时间倒序列出最近
    ``limit`` 条结果。

    Args:
        limit: 最多返回的条数。

    Returns:
        结果字典列表，每项包含 task_id、status、result 等字段。
        若 backend 不是 redis 或查询失败则返回空列表。
    """
    try:
        client = app.backend.client  # redis 后端的客户端连接
    except AttributeError:
        logger.warning("当前 result backend 不是 redis，跳过历史结果查询。")
        return []

    try:
        keys = list(
            client.scan_iter(match="celery-task-meta-*", count=100)
        )[:200]
        results: list[dict[str, Any]] = []
        for key in keys:
            raw = client.get(key)
            if raw is None:
                continue
            data = json.loads(raw)
            data["task_id"] = key.decode().removeprefix("celery-task-meta-")
            results.append(data)
        # redis 返回键的顺序不稳定，按结果中无时间字段，仅按 task_id 倒序示意
        results.sort(key=lambda item: item["task_id"], reverse=True)
        return results[:limit]
    except Exception:  # noqa: BLE001
        logger.exception("查询 redis 中的历史任务结果失败。")
        return []


def show_beat_schedule() -> None:
    """打印当前 celery beat 的定时任务调度表。"""
    logger.info("当前 beat_schedule 定时任务:")
    for name, entry in app.conf.beat_schedule.items():
        logger.info(
            "  - %s: task=%s, schedule=%s, args=%s",
            name,
            entry["task"],
            entry["schedule"],
            entry.get("args"),
        )


def run_db_tasks(timeout: float) -> tuple[dict, dict]:
    """下发数据库相关任务并等待结果。

    Args:
        timeout: 等待结果的最长秒数。

    Returns:
        (try_mysql 结果, get_one_student 结果) 元组。
    """
    logger.info("下发任务 tasks.try_mysql ...")
    mysql_result = try_mysql.delay().get(timeout=timeout)
    print(f"[数据库] tasks.try_mysql = {mysql_result}")

    logger.info("下发任务 tasks.get_one_student(1) ...")
    student_result = get_one_student.delay(1).get(timeout=timeout)
    print(f"[数据库] tasks.get_one_student(1) = {student_result}")
    return mysql_result, student_result


def run_un_task(args: argparse.Namespace) -> list[dict]:
    """单独下发 get_un_groups 任务并等待结果。

    Args:
        args: 命令行参数（count）。

    Returns:
        模型返回的高校信息 JSON 数组。
    """
    logger.info("下发任务 tasks.get_un_groups: count=%s", args.count)
    result = get_un_groups.delay(count=args.count).get(timeout=args.timeout)
    print(f"[高校任务] tasks.get_un_groups({args.count}) =")
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return result


def run_generate_task(args: argparse.Namespace) -> dict:
    """单独下发 generate_many_students 任务并等待结果。

    Args:
        args: 命令行参数（numbers/birthday_min/birthday_max 等）。

    Returns:
        任务执行摘要（inserted、elapsed_seconds 等）。
    """
    logger.info(
        "下发任务 tasks.generate_many_students: numbers=%s, "
        "birthday=[%s, %s], chunk_size=%s, max_workers=%s",
        args.numbers,
        args.birthday_min,
        args.birthday_max,
        args.chunk_size,
        args.max_workers,
    )
    result = generate_many_students.delay(
        numbers=args.numbers,
        birthday_min=args.birthday_min,
        birthday_max=args.birthday_max,
        chunk_size=args.chunk_size,
        max_workers=args.max_workers,
    ).get(timeout=args.timeout)
    print(f"[生成任务] tasks.generate_many_students = {result}")
    return result


def main() -> int:
    """脚本入口：依次执行示例任务演示。

    Returns:
        进程退出码，0 表示成功。
    """
    parser = argparse.ArgumentParser(
        description="alt_celery3 生产者示例脚本"
    )
    parser.add_argument(
        "--task",
        choices=["all", "generate", "un"],
        default="all",
        help=(
            "要执行的任务：all=运行全部示例（默认），"
            "generate=仅运行批量生成学生任务，un=仅运行获取高校信息任务"
        ),
    )
    parser.add_argument(
        "--count",
        type=int,
        default=5,
        help="un 任务要获取的高校数量（默认 5）",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=30.0,
        help="等待任务结果的最长秒数（默认 30；大批量生成时请调大，如 3600）",
    )
    parser.add_argument(
        "--numbers",
        type=int,
        default=1000,
        help="generate 任务要生成的学生人数（默认 1000）",
    )
    parser.add_argument(
        "--birthday-min",
        default="2000-01-01",
        help="generate 任务出生年月日最小值（默认 2000-01-01）",
    )
    parser.add_argument(
        "--birthday-max",
        default="2010-12-31",
        help="generate 任务出生年月日最大值（默认 2010-12-31）",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=50000,
        help="generate 任务单块人数上限（默认 50000）",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=8,
        help="generate 任务并发线程数（默认 8）",
    )
    args = parser.parse_args()

    if args.task == "generate":
        run_generate_task(args)
        return 0

    if args.task == "un":
        run_un_task(args)
        return 0

    show_beat_schedule()

    # 1. 普通任务：异步下发 + 等待结果
    add_result = run_normal_task(2, 3, args.timeout)
    print(f"[普通任务] tasks.add(2, 3) = {add_result}")

    # 2. 定时任务：手动触发一次，演示结果获取（beat 也会周期触发）
    periodic_result = run_scheduled_task(10, 20, args.timeout)
    print(f"[定时任务] tasks.periodic_add(10, 20) = {periodic_result}")

    # 3. 数据库任务：MySQL 连通性测试 + 查询单个学生信息
    run_db_tasks(args.timeout)

    # 4. 从 redis backend 查询定时任务等的历史执行结果
    recent = list_recent_task_results(limit=10)
    if recent:
        print("\n[最近任务结果] (来自 result backend):")
        for item in recent:
            print(
                f"  task_id={item.get('task_id')} "
                f"status={item.get('status')} result={item.get('result')}"
            )
    else:
        print("\n[最近任务结果] 暂无可查询的历史结果。")

    return 0


if __name__ == "__main__":
    sys.exit(main())
