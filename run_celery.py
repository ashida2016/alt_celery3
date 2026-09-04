#!/usr/bin/env python
"""本地启动脚本：一键拉起 celery 全套服务。

用于在没有 Docker 的本地环境中运行 Celery 应用，可选启动：

- ``worker``：任务执行进程
- ``beat``  ：定时任务调度进程
- ``flower``：监控面板（默认 http://localhost:5555）

连接参数（broker / backend）从环境变量 ``CELERY_BROKER_URL`` 与
``CELERY_RESULT_BACKEND`` 读取，可经 ``.env`` 文件自动加载，或直接在
shell 中导出。

用法::

    python run_celery.py                          # 启动 worker + beat
    python run_celery.py --components worker,beat,flower  # 额外启动 flower
    python run_celery.py --flower-port 5566       # 指定 flower 端口

按 ``Ctrl+C`` 优雅停止全部进程。
"""

import argparse
import logging
import os
import signal
import subprocess
import sys
import time
from collections.abc import Mapping

logger = logging.getLogger("run_celery")


def load_dotenv(path: str = ".env") -> None:
    """加载 .env 文件中的环境变量（不覆盖已存在的变量）。

    使用简单的 KEY=VALUE 解析，无需额外依赖；文件不存在时静默跳过。

    Args:
        path: .env 文件路径，默认为当前目录下的 ``.env``。
    """
    if not os.path.isfile(path):
        return
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            key, value = key.strip(), value.strip().strip("'\"")
            os.environ.setdefault(key, value)
    logger.info("已加载环境变量文件: %s", path)


def build_commands(loglevel: str, flower_port: int) -> dict[str, list[str]]:
    """构建各组件的启动命令。

    Args:
        loglevel: celery 日志级别（如 info、debug）。
        flower_port: flower 监控面板端口。

    Returns:
        组件名到命令行列表的映射。
    """
    base = [sys.executable, "-m", "celery", "-A", "app.celery_app"]
    return {
        "worker": base
        + [
            "worker",
            f"--loglevel={loglevel}",
            "--concurrency=4",
            "-Q",
            "default,db",
        ],
        "beat": base + ["beat", f"--loglevel={loglevel}"],
        "flower": base + ["flower", f"--port={flower_port}"],
    }


def shutdown(procs: Mapping[str, subprocess.Popen]) -> None:
    """优雅终止全部子进程。

    Args:
        procs: 组件名到进程对象的映射。
    """
    logger.info("正在停止全部服务...")
    for proc in procs.values():
        if proc.poll() is None:
            proc.terminate()
    for proc in procs.values():
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
    logger.info("全部服务已停止。")


def main() -> int:
    """脚本入口：启动所选组件并阻塞等待，直到收到中断信号。

    Returns:
        进程退出码，0 表示正常退出。
    """
    parser = argparse.ArgumentParser(description="本地启动 celery 全套服务")
    parser.add_argument(
        "--components",
        default="worker,beat",
        help="要启动的组件，逗号分隔（默认 worker,beat；flower 可按需加入）",
    )
    parser.add_argument(
        "--loglevel",
        default="info",
        help="celery 日志级别（默认 info）",
    )
    parser.add_argument(
        "--flower-port",
        type=int,
        default=5555,
        help="flower 监控面板端口（默认 5555）",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    load_dotenv()

    commands = build_commands(args.loglevel, args.flower_port)
    names = [n.strip() for n in args.components.split(",") if n.strip()]
    unknown = [n for n in names if n not in commands]
    if unknown:
        parser.error(
            f"未知组件: {', '.join(unknown)}（可选: worker, beat, flower）"
        )

    # 启动各组件子进程
    procs: dict[str, subprocess.Popen] = {}
    for name in names:
        cmd = commands[name]
        logger.info("启动 %s: %s", name, " ".join(cmd))
        procs[name] = subprocess.Popen(cmd)

    # 收到 SIGTERM 时转为 KeyboardInterrupt，统一走优雅退出路径
    def _handle_signal(signum, _frame):  # noqa: ANN001
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, _handle_signal)

    try:
        while True:
            time.sleep(1)
            # 任一子进程异常退出则立即停止全部服务
            for name, proc in procs.items():
                if proc.poll() is not None:
                    logger.error(
                        "组件 %s 异常退出（code=%s），停止全部服务。",
                        name,
                        proc.returncode,
                    )
                    shutdown(procs)
                    return 1
    except KeyboardInterrupt:
        logger.info("收到中断信号。")
        shutdown(procs)
    return 0


if __name__ == "__main__":
    sys.exit(main())
