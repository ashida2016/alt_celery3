"""数据库初始化任务模块。

任务 ``tasks.init_web_db`` 重建 web_db / log_db 数据库与对应用户，
并在 web_db 内创建全部业务表（面向千万级学生数据做了性能优化）。

危险操作说明：
- 会 DROP 旧的 web_db / log_db 及用户 web_user / log_user
- 必须显式传入 ``confirm=True`` 才会执行
- 需要管理员账号（环境变量 ``MYSQL_ADMIN_USER`` / ``MYSQL_ADMIN_PASSWORD``，
  账号需具备全局 DROP/CREATE 权限）

性能优化措施（学生数据可达千万行以上）：
- 全部表使用 InnoDB + BIGINT 自增主键 + utf8mb4_unicode_ci
- 大表（成绩、入学关系）不建外键，避免千万级批量写入时的外键
  校验开销，引用完整性由应用层保证；仅建二级索引加速查询
- students 增加 name / birthday 二级索引；gender 低基数不单独建索引
- 一人一条的表（高考成绩、毕业成绩）对学生 ID 建唯一约束
- 入学关系表以 (student_id, enroll_year) 唯一约束防止重复入学记录
"""

import os

from scdb_mysql_speed import SCDBMySQLMeta, SCDBMySQLSpeed
from sclog_lite import logger

from app.celery_app import app
from app.log_setup import init_logging

# 确保任务进程内日志中间件已初始化（幂等）
init_logging()

# 业务表 DDL（key 为表名，保持创建顺序：被依赖表在前）
BUSINESS_TABLE_DDL: dict[str, str] = {
    "students": (
        "CREATE TABLE IF NOT EXISTS students ("
        " id BIGINT NOT NULL AUTO_INCREMENT COMMENT '主键',"
        " name VARCHAR(50) NOT NULL COMMENT '姓名',"
        " birthday DATE NOT NULL COMMENT '出生日期',"
        " gender CHAR(1) NOT NULL COMMENT '性别: M=男, F=女',"
        " status TINYINT NOT NULL DEFAULT 0"
        " COMMENT '入学状态: 0=未高考, 10=已高考未入学, 20=在读, 30=已毕业',"
        " created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP"
        " COMMENT '创建时间',"
        " PRIMARY KEY (id),"
        " KEY idx_name (name),"
        " KEY idx_birthday (birthday)"
        ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci"
        " COMMENT='学生信息表'"
    ),
    "universities": (
        "CREATE TABLE IF NOT EXISTS universities ("
        " id BIGINT NOT NULL AUTO_INCREMENT COMMENT '主键',"
        " name VARCHAR(100) NOT NULL COMMENT '高校名称',"
        " code CHAR(10) NOT NULL COMMENT '高校代码（五位数字）',"
        " type VARCHAR(10) NOT NULL COMMENT '高校类型：民办/公办',"
        " nature VARCHAR(10) NOT NULL COMMENT '高校性质：985/211/一本/其他',"
        " created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP"
        " COMMENT '创建时间',"
        " PRIMARY KEY (id),"
        " UNIQUE KEY uk_name (name),"
        " UNIQUE KEY uk_code (code)"
        ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci"
        " COMMENT='高校信息表'"
    ),
    "major_groups": (
        "CREATE TABLE IF NOT EXISTS major_groups ("
        " id BIGINT NOT NULL AUTO_INCREMENT COMMENT '主键',"
        " university_id BIGINT NOT NULL COMMENT '所属高校 ID',"
        " name VARCHAR(100) NOT NULL COMMENT '专业组名称',"
        " code CHAR(10) NOT NULL COMMENT '专业组代码（五位数字）',"
        " created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP"
        " COMMENT '创建时间',"
        " PRIMARY KEY (id),"
        " UNIQUE KEY uk_uni_name (university_id, name),"
        " UNIQUE KEY uk_uni_code (university_id, code),"
        " CONSTRAINT fk_major_groups_universities"
        " FOREIGN KEY (university_id) REFERENCES universities (id)"
        " ON DELETE CASCADE ON UPDATE CASCADE"
        ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci"
        " COMMENT='专业组信息表'"
    ),
    "gaokao_scores": (
        "CREATE TABLE IF NOT EXISTS gaokao_scores ("
        " id BIGINT NOT NULL AUTO_INCREMENT COMMENT '主键',"
        " student_id BIGINT NOT NULL COMMENT '学生 ID',"
        " score INT NOT NULL COMMENT '高考总分',"
        " exam_date DATE NOT NULL COMMENT '高考日期',"
        " created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP"
        " COMMENT '创建时间',"
        " PRIMARY KEY (id),"
        " UNIQUE KEY uk_student (student_id),"
        " KEY idx_exam_date (exam_date)"
        ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci"
        " COMMENT='学生成绩表-高考成绩表'"
    ),
    "undergraduate_scores": (
        "CREATE TABLE IF NOT EXISTS undergraduate_scores ("
        " id BIGINT NOT NULL AUTO_INCREMENT COMMENT '主键',"
        " student_id BIGINT NOT NULL COMMENT '学生 ID',"
        " academic_year VARCHAR(9) NOT NULL COMMENT '学年，如 2023-2024',"
        " subject VARCHAR(50) NOT NULL COMMENT '课程名称',"
        " score DECIMAL(5, 2) NOT NULL COMMENT '考试成绩',"
        " exam_date DATE NOT NULL COMMENT '考试日期',"
        " created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP"
        " COMMENT '创建时间',"
        " PRIMARY KEY (id),"
        " KEY idx_student_year (student_id, academic_year),"
        " KEY idx_subject (subject)"
        ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci"
        " COMMENT='学生成绩表-本科成绩表'"
    ),
    "graduation_scores": (
        "CREATE TABLE IF NOT EXISTS graduation_scores ("
        " id BIGINT NOT NULL AUTO_INCREMENT COMMENT '主键',"
        " student_id BIGINT NOT NULL COMMENT '学生 ID',"
        " gpa DECIMAL(4, 2) NOT NULL COMMENT '绩点',"
        " graduate_date DATE NOT NULL COMMENT '毕业日期',"
        " created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP"
        " COMMENT '创建时间',"
        " PRIMARY KEY (id),"
        " UNIQUE KEY uk_student (student_id),"
        " KEY idx_graduate_date (graduate_date)"
        ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci"
        " COMMENT='学生成绩表-毕业成绩表'"
    ),
    "student_enrollments": (
        "CREATE TABLE IF NOT EXISTS student_enrollments ("
        " id BIGINT NOT NULL AUTO_INCREMENT COMMENT '主键',"
        " student_id BIGINT NOT NULL COMMENT '学生 ID',"
        " university_id BIGINT NOT NULL COMMENT '高校 ID',"
        " major_group_id BIGINT NOT NULL COMMENT '专业组 ID',"
        " enroll_year SMALLINT NOT NULL COMMENT '入学学年，如 2023',"
        " created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP"
        " COMMENT '创建时间',"
        " PRIMARY KEY (id),"
        " UNIQUE KEY uk_student_enroll (student_id, enroll_year),"
        " KEY idx_uni_major (university_id, major_group_id),"
        " KEY idx_enroll_year (enroll_year)"
        ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci"
        " COMMENT='学生-高校-专业组入学关系表'"
    ),
}

# log_db 中 sclog 日志表 DDL（重建 log_db 后恢复日志持久化）
_APP_LOGS_DDL = (
    "CREATE TABLE IF NOT EXISTS app_logs ("
    " id BIGINT NOT NULL AUTO_INCREMENT,"
    " level VARCHAR(16) NOT NULL,"
    " message TEXT NOT NULL,"
    " created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,"
    " logger_name VARCHAR(128) NOT NULL DEFAULT '',"
    " file_path VARCHAR(255) NOT NULL DEFAULT '',"
    " line_number INT NOT NULL DEFAULT 0,"
    " exception TEXT NULL,"
    " PRIMARY KEY (id),"
    " KEY idx_created_at (created_at)"
    ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci"
    " COMMENT='应用日志表'"
)


def _admin_meta() -> SCDBMySQLMeta:
    """构建管理员连接配置（不指定数据库）。

    Returns:
        管理员账号的连接配置对象。
    """
    user = os.environ.get("MYSQL_ADMIN_USER", "root")
    password = os.environ.get("MYSQL_ADMIN_PASSWORD", "")
    if not password:
        logger.warning(
            "[init_web_db] MYSQL_ADMIN_PASSWORD 未配置，"
            "若管理员账号认证失败请填写该环境变量。"
        )
    return SCDBMySQLMeta(
        host=os.environ.get("APP_DB_HOST", "192.168.1.101"),
        port=int(os.environ.get("APP_DB_PORT", "3306")),
        user=user,
        password=password,
        charset="utf8mb4",
    )


def _recreate_databases_and_users(db: SCDBMySQLSpeed) -> list[str]:
    """删除并重建 web_db / log_db 及用户 web_user / log_user。

    Args:
        db: 管理员连接池句柄。

    Returns:
        执行的操作列表（用于任务返回值展示）。
    """
    operations: list[str] = []

    web_password = os.environ.get("APP_DB_PASSWORD", "")
    log_password = os.environ.get("LOG_DB_PASSWORD", "")

    # 1. 删除旧库与旧用户（用户可能存在于 % 或 localhost 两个 host）
    for ddl in (
        "DROP DATABASE IF EXISTS web_db",
        "DROP DATABASE IF EXISTS log_db",
        "DROP USER IF EXISTS 'web_user'@'%'",
        "DROP USER IF EXISTS 'web_user'@'localhost'",
        "DROP USER IF EXISTS 'log_user'@'%'",
        "DROP USER IF EXISTS 'log_user'@'localhost'",
    ):
        db.execute(ddl)
        operations.append(ddl)
    logger.info("[init_web_db] 旧数据库与用户已删除")

    # 2. 重建数据库
    for ddl in (
        "CREATE DATABASE web_db CHARACTER SET utf8mb4"
        " COLLATE utf8mb4_unicode_ci",
        "CREATE DATABASE log_db CHARACTER SET utf8mb4"
        " COLLATE utf8mb4_unicode_ci",
    ):
        db.execute(ddl)
        operations.append(ddl)

    # 3. 重建用户并授权（web_user 管理 web_db，log_user 管理 log_db）
    # 密码来自 .env，与各任务使用的连接凭据保持一致
    grants = (
        (
            f"CREATE USER 'web_user'@'%' IDENTIFIED BY '{web_password}'",
            "GRANT ALL PRIVILEGES ON web_db.* TO 'web_user'@'%'",
        ),
        (
            f"CREATE USER 'log_user'@'%' IDENTIFIED BY '{log_password}'",
            "GRANT ALL PRIVILEGES ON log_db.* TO 'log_user'@'%'",
        ),
    )
    for create_user_ddl, grant_ddl in grants:
        db.execute(create_user_ddl)
        db.execute(grant_ddl)
        operations.append(create_user_ddl.split(" IDENTIFIED")[0])
        operations.append(grant_ddl)
    db.execute("FLUSH PRIVILEGES")
    operations.append("FLUSH PRIVILEGES")

    logger.info("[init_web_db] 数据库与用户重建完成")
    return operations


def _create_business_tables(db: SCDBMySQLSpeed) -> list[str]:
    """在 web_db 内创建全部业务表。

    Args:
        db: web_user 连接池句柄（已连接 web_db）。

    Returns:
        成功创建的表名列表。
    """
    created: list[str] = []
    for table_name, ddl in BUSINESS_TABLE_DDL.items():
        db.execute(ddl)
        created.append(table_name)
        logger.info("[init_web_db] 业务表已创建: {}", table_name)
    return created


def _restore_log_db_tables() -> None:
    """在 log_db 内重建 sclog 日志表，恢复日志持久化能力。"""
    meta = SCDBMySQLMeta(
        host=os.environ.get("LOG_DB_HOST", "192.168.1.101"),
        port=int(os.environ.get("LOG_DB_PORT", "3306")),
        user=os.environ.get("LOG_DB_USER", "log_user"),
        password=os.environ.get("LOG_DB_PASSWORD", ""),
        database=os.environ.get("LOG_DB_NAME", "log_db"),
        charset="utf8mb4",
    )
    db = SCDBMySQLSpeed(meta)
    try:
        db.execute(_APP_LOGS_DDL)
        logger.info("[init_web_db] log_db 日志表 app_logs 已重建")
    finally:
        db.close()


def _build_web_meta() -> SCDBMySQLMeta:
    """构建 web_user 连接 web_db 的配置。

    Returns:
        web_user 的连接配置对象。
    """
    return SCDBMySQLMeta(
        host=os.environ.get("APP_DB_HOST", "192.168.1.101"),
        port=int(os.environ.get("APP_DB_PORT", "3306")),
        user=os.environ.get("APP_DB_USER", "web_user"),
        password=os.environ.get("APP_DB_PASSWORD", ""),
        database=os.environ.get("APP_DB_NAME", "web_db"),
        charset="utf8mb4",
    )


@app.task(name="tasks.init_web_db")
def init_web_db(confirm: bool = False) -> dict:
    """初始化数据库：重建 web_db / log_db 及用户，并创建业务表。

    危险操作：会删除旧的 web_db / log_db 与用户 web_user / log_user，
    必须显式传入 ``confirm=True`` 才会执行。

    Args:
        confirm: 危险操作确认开关，必须为 True。

    Returns:
        执行摘要::

            {"operations": [...], "databases": ["web_db", "log_db"],
             "tables": ["students", ...], "log_tables": ["app_logs"]}

    Raises:
        ValueError: confirm 未显式传 True 时抛出。
        SCDBConnectionError: 管理员账号认证失败时抛出。
    """
    if not confirm:
        raise ValueError(
            "init_web_db 是危险操作（会删除旧库与用户），"
            "必须显式传入 confirm=True 才能执行"
        )

    logger.info("[init_web_db] 开始初始化数据库 ...")
    db = SCDBMySQLSpeed(_admin_meta())
    try:
        operations = _recreate_databases_and_users(db)
    finally:
        db.close()

    # 以 web_user 身份在 web_db 创建业务表
    web_db = SCDBMySQLSpeed(_build_web_meta())
    try:
        tables = _create_business_tables(web_db)
    finally:
        web_db.close()

    # 以 log_user 身份重建 log_db 日志表，恢复 sclog 持久化
    _restore_log_db_tables()

    summary = {
        "operations": operations,
        "databases": ["web_db", "log_db"],
        "tables": tables,
        "log_tables": ["app_logs"],
    }
    logger.info("[init_web_db] 初始化完成: tables={}", tables)
    return summary
