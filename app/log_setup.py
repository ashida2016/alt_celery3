"""应用日志初始化模块（基于 sclog_lite）。

按照 sclog_lite 的 Web 应用集成模式：在应用启动时初始化一次日志
中间件——控制台/文件日志（loguru），并添加 MySQL sink 将日志异步
批量持久化到日志库（log_db）。日志库连接信息从 .env / 环境变量
读取（LOG_DB_*）。

特性（由 sclog_lite 保证）：
- 故障隔离：日志库不可用时不影响主应用，日志回退写入本地 JSONL
- 异步批量写入：queue + 后台线程 + 连接池
- 进程退出时自动 flush 队列剩余日志

用法::

    from app.log_setup import init_logging
    init_logging()          # 幂等，可安全重复调用
    from sclog_lite import logger
    logger.info("hello")
"""

import contextlib
import os

from sclog_lite import add_mysql_sink, logger, setup_logging

# 日志文件目录（容器内以 celeuser 运行，需保证该目录可写）
LOG_DIR: str = os.environ.get("LOG_DIR", "logs")
FALLBACK_PATH: str = os.path.join(LOG_DIR, "mysql_fallback.jsonl")

_initialized: bool = False


def _log_db_config() -> dict:
    """从环境变量读取日志库连接配置。

    Returns:
        包含 host/port/user/password/database 的配置字典。
    """
    return {
        "host": os.environ.get("LOG_DB_HOST", "192.168.1.101"),
        "port": int(os.environ.get("LOG_DB_PORT", "3306")),
        "user": os.environ.get("LOG_DB_USER", "log_user"),
        "password": os.environ.get("LOG_DB_PASSWORD", ""),
        "database": os.environ.get("LOG_DB_NAME", "log_db"),
    }


def init_logging() -> None:
    """初始化应用日志中间件（幂等）。

    按应用集成模式依次执行：
    1. setup_logging：启用控制台 INFO 与文件 DEBUG（带轮转）日志
    2. add_mysql_sink：添加 MySQL 异步批量持久化 sink

    各 sink 相互独立：任一组件初始化失败不影响其余组件，
    且任何失败都不抛出异常（日志属于旁路组件，不应阻断主应用）。
    """
    global _initialized
    if _initialized:
        return

    errors: list[str] = []

    with contextlib.suppress(OSError):
        os.makedirs(LOG_DIR, exist_ok=True)  # 目录创建失败时跳过文件日志，不影响 sink

    # 1. 控制台/文件日志（文件写入失败时降级为仅控制台输出）
    try:
        setup_logging(
            console=True,
            console_level="INFO",
            file_path=os.path.join(LOG_DIR, "sclog.log"),
            file_level="DEBUG",
            file_rotation="10 MB",
            file_retention="10 days",
        )
    except Exception as exc:  # noqa: BLE001
        errors.append(f"file sink: {exc!r}")
        with contextlib.suppress(Exception):
            setup_logging(console=True, console_level="INFO", file_path=False)

    # 2. MySQL 数据库 sink（独立于文件日志，目录权限问题不影响其初始化）
    try:
        add_mysql_sink(
            **_log_db_config(),
            table_name="app_logs",
            auto_create_table=True,
            batch_size=50,
            batch_interval=1.0,
            fallback_path=FALLBACK_PATH,
            level="INFO",
        )
    except Exception as exc:  # noqa: BLE001
        errors.append(f"mysql sink: {exc!r}")

    _initialized = True
    if errors:
        print(f"[log_setup] 日志中间件部分组件初始化失败: {'; '.join(errors)}")
    else:
        logger.info("日志中间件初始化完成（console/file/mysql sink）。")
