"""数据库相关任务模块。

提供 MySQL 业务库连通性测试任务（try_mysql）与单学生信息查询任务
（get_one_student）。两者均通过 sclog（sclog_lite）记录操作日志，
数据库连接信息从 .env / 环境变量读取（APP_DB_*）。
"""

import os

from scdb_mysql_speed import SCDBMySQLMeta, SCDBMySQLSpeed
from sclog_lite import logger

from app.celery_app import app
from app.log_setup import init_logging

# 确保任务进程内日志中间件已初始化（幂等）
init_logging()


def _build_meta() -> SCDBMySQLMeta:
    """从环境变量构建业务库连接配置。

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
