"""数据库相关任务模块。

提供 MySQL 业务库连通性测试任务（try_mysql）与单学生信息查询任务
（get_one_student）。两者均通过 sclog（sclog_lite）记录操作日志，
数据库连接信息从 .env / 环境变量读取（APP_DB_*）。
"""

import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

from alt_generate_zh_name import generate as generate_zh_students
from scdb_mysql_speed import SCDBMySQLMeta, SCDBMySQLSpeed
from sclog_lite import logger

from app.celery_app import app
from app.log_setup import init_logging

# 确保任务进程内日志中间件已初始化（幂等）
init_logging()

# 学生信息批量插入 SQL（占位符风格，防注入）
INSERT_STUDENT_SQL = (
    "INSERT INTO students (name, gender, birthday) VALUES (%s, %s, %s)"
)


def _build_meta(pool_size: int = 5) -> SCDBMySQLMeta:
    """从环境变量构建业务库连接配置。

    Args:
        pool_size: 连接池大小（多线程写入时按并发线程数设置）。

    Returns:
        scdb_mysql_speed 的连接配置对象。
    """
    return SCDBMySQLMeta(
        host=os.environ.get("APP_DB_HOST", "192.168.1.101"),
        port=int(os.environ.get("APP_DB_PORT", "3306")),
        user=os.environ.get("APP_DB_USER", "web_user"),
        password=os.environ.get("APP_DB_PASSWORD", ""),
        database=os.environ.get("APP_DB_NAME", "web_db"),
        charset="utf8mb4",
        pool_size=pool_size,
        pool_max_overflow=pool_size,
    )


@app.task(name="tasks.try_mysql", bind=True, max_retries=3)
def try_mysql(self) -> dict:
    """测试 MySQL 业务库（web_db）连通性。

    Args:
        self: Celery 任务实例（bind=True 时自动注入）。

    Returns:
        连通性测试结果::

            {"ok": True, "database": "web_db"}
            {"ok": False, "database": "web_db", "error": "..."}
    """
    logger.info("[try_mysql] 开始测试 MySQL 业务库连通性 ...")
    meta = _build_meta()
    try:
        db = SCDBMySQLSpeed(meta)
        ok = db.test_connection()
        db.close()
    except Exception as exc:  # noqa: BLE001
        logger.error("[try_mysql] MySQL 连接失败: {}", exc)
        return {"ok": False, "database": meta.database, "error": str(exc)}

    logger.info("[try_mysql] 测试结果: ok={}", ok)
    return {"ok": ok, "database": meta.database}


@app.task(name="tasks.get_one_student")
def get_one_student(student_id: int) -> dict:
    """查询单个学生信息。

    Args:
        student_id: 学生记录主键 id（students 表）。

    Returns:
        查询结果::

            {"found": True, "student": {"id": 1, "name": "...", ...}}
            {"found": False, "student_id": 1, "error": "..."}  # 未找到或出错
    """
    logger.info("[get_one_student] 查询学生信息: id={}", student_id)
    try:
        db = SCDBMySQLSpeed(_build_meta())
        rows = db.fetch_all(
            "SELECT id, name, gender, birthday FROM students WHERE id = %s LIMIT 1",
            (student_id,),
            result_format="dict",
        )
        db.close()
    except Exception as exc:  # noqa: BLE001
        logger.error("[get_one_student] 查询失败: id={}, err={}", student_id, exc)
        return {"found": False, "student_id": student_id, "error": str(exc)}

    if not rows:
        logger.warning("[get_one_student] 学生不存在: id={}", student_id)
        return {"found": False, "student_id": student_id}

    student = rows[0]
    logger.info("[get_one_student] 查询成功: {}", student)
    return {"found": True, "student": student}


def _split_chunks(numbers: int, chunk_size: int) -> list[int]:
    """把总生成数量切分为分块列表。

    Args:
        numbers: 总人数。
        chunk_size: 单块人数上限。

    Returns:
        每块人数组成的列表，所有块之和等于 numbers。
    """
    full, remainder = divmod(numbers, chunk_size)
    chunks = [chunk_size] * full
    if remainder:
        chunks.append(remainder)
    return chunks


def _generate_and_insert_chunk(
    db: SCDBMySQLSpeed,
    size: int,
    birthday_min: str,
    birthday_max: str,
) -> int:
    """生成单块学生数据并批量入库（工作线程内执行）。

    Args:
        db: 共享的数据库连接池句柄（PooledDB 线程安全）。
        size: 本块生成人数。
        birthday_min: 生日最小值（YYYY-MM-DD）。
        birthday_max: 生日最大值（YYYY-MM-DD）。

    Returns:
        本块实际插入的行数。
    """
    df = generate_zh_students(
        size, birth_start=birthday_min, birth_end=birthday_max
    )
    rows: list[tuple[Any, ...] | list[Any] | dict[Any, Any]] = [
        (row["name"], row["gender"], row["birthday"].strftime("%Y-%m-%d"))
        for _, row in df.iterrows()
    ]
    inserted = db.execute_many(INSERT_STUDENT_SQL, rows)
    logger.debug("[generate_many_students] 分块入库完成: {} 条", inserted)
    return inserted


@app.task(
    name="tasks.generate_many_students",
    soft_time_limit=1800,
    time_limit=1900,
)
def generate_many_students(
    numbers: int,
    birthday_min: str = "2000-01-01",
    birthday_max: str = "2010-12-31",
    chunk_size: int = 50_000,
    max_workers: int = 8,
) -> dict:
    """批量生成随机学生信息并写入 web_db.students 表。

    面向百万级数据量的多线程性能优化：

    - 总量按 chunk_size 切分为多个分块
    - ThreadPoolExecutor 并发执行各分块，每线程独立完成
      "生成（alt_generate_zh_name）+ 批量入库（execute_many）"
    - 共享 scdb_mysql_speed 连接池（pool_size=max_workers），
      连接由池复用，避免每块重建连接

    Args:
        numbers: 要生成的学生总人数。
        birthday_min: 出生年月日最小值（YYYY-MM-DD）。
        birthday_max: 出生年月日最大值（YYYY-MM-DD）。
        chunk_size: 单块人数上限（默认 50000，兼顾内存与批写效率）。
        max_workers: 并发线程数（默认 8，连接池同步扩容）。

    Returns:
        执行摘要::

            {"inserted": 1000000, "numbers": 1000000,
             "chunk_size": 50000, "max_workers": 8,
             "elapsed_seconds": 123.45, "rows_per_second": 8100.4}
    """
    if numbers <= 0:
        raise ValueError(f"numbers 必须为正整数，收到: {numbers}")
    max_workers = max(1, min(max_workers, 32))
    chunk_size = max(1, min(chunk_size, numbers))

    logger.info(
        "[generate_many_students] 开始: numbers={}, range=[{}, {}], "
        "chunk_size={}, max_workers={}",
        numbers,
        birthday_min,
        birthday_max,
        chunk_size,
        max_workers,
    )

    db = SCDBMySQLSpeed(_build_meta(pool_size=max_workers))
    started = time.monotonic()
    inserted = 0
    try:
        chunks = _split_chunks(numbers, chunk_size)
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = [
                pool.submit(
                    _generate_and_insert_chunk,
                    db,
                    size,
                    birthday_min,
                    birthday_max,
                )
                for size in chunks
            ]
            for future in as_completed(futures):
                inserted += future.result()
                progress = inserted * 100 // numbers
                if progress % 10 == 0:
                    logger.info(
                        "[generate_many_students] 进度: {}/{} ({}%)",
                        inserted,
                        numbers,
                        progress,
                    )
    finally:
        db.close()

    elapsed = time.monotonic() - started
    summary = {
        "inserted": inserted,
        "numbers": numbers,
        "chunk_size": chunk_size,
        "max_workers": max_workers,
        "elapsed_seconds": round(elapsed, 2),
        "rows_per_second": round(inserted / elapsed, 1) if elapsed else 0.0,
    }
    logger.info("[generate_many_students] 完成: {}", summary)
    return summary
