"""高校信息任务模块。

任务 ``tasks.get_un_groups`` 通过硅基流动（SiliconFlow）的 Chat
Completion 接口生成指定数量的中国高校信息（含下属专业组），并对
名称查重后写入 web_db 的 universities / major_groups 表。

数据库表已预先存在（结构由项目方维护），本模块不做建表操作。
"""

import json
from typing import Any

from scdb_mysql_speed import SCDBMySQLSpeed
from sclog_lite import logger

from app.celery_app import app
from app.gjld_api import gjld_chat_completion
from app.log_setup import init_logging
from app.tasks.db_tasks import _build_meta

# 确保任务进程内日志中间件已初始化（幂等）
init_logging()

# 高校信息 JSON 输出格式示例（固化在提示词中，约束模型输出）
_JSON_EXAMPLE = """[
  {
    "name": "上海建桥学院",
    "code": "10299",
    "type": "民办",
    "nature": "其他",
    "majors": [
      {"name": "计算机科学与技术", "code": "080901"},
      {"name": "软件工程", "code": "080902"}
    ]
  }
]"""

# 高校信息提示词模板
UN_PROMPT_TEMPLATE = (
    "请列举 {count} 所中国真实存在的本科高校信息，要求覆盖不同类型"
    "（民办/公办）与不同性质（985/211/一本/其他），每所高校给出 3-5 个"
    "有代表性的专业组。name 必须是真实高校全称，code 使用真实的高校"
    "院校标识码（5 位数字），专业组 code 使用五位数字代码。\n"
    "严格按照如下 JSON 数组格式输出，禁止输出任何解释、前缀或 markdown "
    "代码块标记，直接输出 JSON：\n" + _JSON_EXAMPLE
)


def _extract_json(text: str) -> list[dict[str, Any]]:
    """从模型返回文本中提取 JSON 数组并做基本校验。

    兼容模型输出 markdown 代码块标记（```json ... ```）的情况；
    逐条校验字段，丢弃缺少 name 的无效条目。

    Args:
        text: 模型返回的原始文本。

    Returns:
        校验通过的高校信息字典列表。
    """
    cleaned = text.strip()
    if cleaned.startswith("```"):
        # 去掉 markdown 代码块围栏
        cleaned = cleaned.strip("`")
        if cleaned.lower().startswith("json"):
            cleaned = cleaned[4:]
    data = json.loads(cleaned.strip())
    if not isinstance(data, list):
        raise ValueError("模型输出不是 JSON 数组")

    valid: list[dict[str, Any]] = []
    for item in data:
        if not isinstance(item, dict) or not item.get("name"):
            continue
        majors = [
            {"name": m.get("name", ""), "code": m.get("code", "")}
            for m in item.get("majors", [])
            if isinstance(m, dict) and m.get("name")
        ]
        valid.append(
            {
                "name": str(item["name"]).strip(),
                "code": str(item.get("code", "")).strip(),
                "type": str(item.get("type", "")).strip(),
                "nature": str(item.get("nature", "")).strip(),
                "majors": majors,
            }
        )
    return valid


def _sanitize_code(code: str, width: int = 5) -> str:
    """清洗代码字段：仅保留数字并截断到表结构允许的宽度。

    表结构中 universities.code / major_groups.code 均为 char(5)，
    模型可能返回超长代码，入库前截断；返回给调用方的 JSON
    仍保留模型原始代码。

    Args:
        code: 原始代码字符串。
        width: 目标宽度（默认 5）。

    Returns:
        清洗后的代码字符串。
    """
    digits = "".join(ch for ch in code if ch.isdigit())
    return digits[:width]


def _insert_universities(
    db: SCDBMySQLSpeed, universities: list[dict[str, Any]]
) -> dict[str, int]:
    """高校及专业组查重入库（以名称为准，重复不添加）。

    同时兼容表结构的唯一约束：高校 code 全局唯一、专业组
    (university_id, code) 唯一，冲突的条目记日志后跳过。

    Args:
        db: 数据库连接池句柄。
        universities: 校验后的高校信息列表。

    Returns:
        统计信息::

            {"university_new": 2, "university_dup": 1,
             "major_new": 6, "major_dup": 0}
    """
    # 1. 高校查重：已有名称 / 代码集合
    un_rows = db.fetch_all(
        "SELECT name, code FROM universities", result_format="dict"
    )
    existing_universities = {row["name"] for row in un_rows}
    existing_un_codes = {row["code"] for row in un_rows}

    stats = {
        "university_new": 0,
        "university_dup": 0,
        "major_new": 0,
        "major_dup": 0,
    }

    for un in universities:
        if un["name"] in existing_universities:
            stats["university_dup"] += 1
            logger.info("[get_un_groups] 高校已存在，跳过: {}", un["name"])
            continue

        un_code = _sanitize_code(un["code"], width=5)
        if un_code and un_code in existing_un_codes:
            stats["university_dup"] += 1
            logger.warning(
                "[get_un_groups] 高校代码冲突，跳过: {} (code={})",
                un["name"],
                un_code,
            )
            continue

        db.execute(
            "INSERT INTO universities (name, code, type, nature) "
            "VALUES (%s, %s, %s, %s)",
            (un["name"], un_code, un["type"], un["nature"]),
        )
        existing_universities.add(un["name"])
        existing_un_codes.add(un_code)
        stats["university_new"] += 1
        logger.info("[get_un_groups] 高校入库: {}", un["name"])

        # 2. 专业组查重：该校已有名称 / 代码集合
        un_row = db.fetch_all(
            "SELECT id FROM universities WHERE name = %s LIMIT 1",
            (un["name"],),
            result_format="dict",
        )
        university_id = un_row[0]["id"]
        mg_rows = db.fetch_all(
            "SELECT name, code FROM major_groups WHERE university_id = %s",
            (university_id,),
            result_format="dict",
        )
        existing_majors = {row["name"] for row in mg_rows}
        existing_mg_codes = {row["code"] for row in mg_rows}

        for major in un["majors"]:
            if major["name"] in existing_majors:
                stats["major_dup"] += 1
                continue
            major_code = _sanitize_code(major["code"])
            if major_code and major_code in existing_mg_codes:
                stats["major_dup"] += 1
                logger.warning(
                    "[get_un_groups] 专业组代码冲突，跳过: {} (code={})",
                    major["name"],
                    major_code,
                )
                continue
            try:
                db.execute(
                    "INSERT INTO major_groups (university_id, name, code) "
                    "VALUES (%s, %s, %s)",
                    (university_id, major["name"], major_code),
                )
            except Exception as exc:  # noqa: BLE001
                # 单条失败不阻断整体入库
                logger.warning(
                    "[get_un_groups] 专业组入库失败，跳过: {} - {}",
                    major["name"],
                    exc,
                )
                continue
            existing_majors.add(major["name"])
            existing_mg_codes.add(major_code)
            stats["major_new"] += 1

    return stats


@app.task(name="tasks.get_un_groups")
def get_un_groups(count: int = 5) -> list[dict[str, Any]]:
    """自动获取指定数量的高校信息（含专业组），查重后入库。

    流程：
    1. 以固化的问题模板调用 ``gjld_chat_completion`` 生成高校信息
    2. 解析并校验模型返回的标准 JSON 数组
    3. 以名称为准查重（重复不添加）后写入
       ``universities`` / ``major_groups`` 表

    Args:
        count: 要获取的高校数量（默认 5）。

    Returns:
        标准的 JSON 对象（高校信息数组），格式见项目 README 示例。
    """
    if count <= 0:
        raise ValueError(f"count 必须为正整数，收到: {count}")

    logger.info("[get_un_groups] 开始获取 {} 所高校信息 ...", count)
    text = gjld_chat_completion(
        UN_PROMPT_TEMPLATE.replace("{count}", str(count)),
        system="你是一个严谨的中国高校信息数据库助手，只输出合法 JSON。",
        temperature=0.3,
        timeout=180.0,
    )
    universities = _extract_json(text)
    logger.info(
        "[get_un_groups] 模型返回并校验通过 {} 条高校信息", len(universities)
    )

    db = SCDBMySQLSpeed(_build_meta())
    try:
        stats = _insert_universities(db, universities)
    finally:
        db.close()

    logger.info("[get_un_groups] 入库统计: {}", stats)
    return universities
