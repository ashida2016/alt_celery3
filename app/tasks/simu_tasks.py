"""高校业务模拟任务模块。

提供覆盖学生「高考 → 录取 → 在读考试 → 毕业」全生命周期的一组
模拟任务，均支持多线程分块处理，面向千万级学生数据优化：

- ``tasks.simu_ncee``     ：模拟高考（登记高考成绩，状态 0→10）
- ``tasks.simu_admission``：高校录取（登记入学关系，状态 10→20）
- ``tasks.simu_exam``     ：高校日常考试（登记本科成绩）
- ``tasks.simu_graduate`` ：本科毕业（计算绩点，状态 20→30）

通用性能设计：
- 按 ID 区间把目标学生切分为多个窗口，ThreadPoolExecutor 并发处理
- 各线程共享 scdb_mysql_speed 连接池（pool_size=max_workers）
- 批量写入使用 execute_many，状态流转使用单条批量 UPDATE
"""

import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

from alt_celery3_contract.constants import TaskName
from alt_celery3_contract.schemas import SimuSchoolYearPayload
from scdb_mysql_speed import SCDBMySQLSpeed
from sclog_lite import logger

from app.celery_app import app
from app.log_setup import init_logging
from app.tasks.db_tasks import _build_meta

# 确保任务进程内日志中间件已初始化（幂等）
init_logging()

# 高考分数分布参数：正态分布，均值 530，标准差 50，区间 [400, 660]
GAOKAO_MEAN = 530
GAOKAO_STD = 50
GAOKAO_MIN = 400
GAOKAO_MAX = 660

# 本科日常考试分数分布参数：正态分布，均值 75，标准差 15，区间 [0, 100]
EXAM_MEAN = 75
EXAM_STD = 15
EXAM_MIN = 0
EXAM_MAX = 100

# 本科课程池（日常考试随机抽取）
SUBJECTS = (
    "高等数学",
    "大学英语",
    "数据结构",
    "线性代数",
    "大学物理",
    "操作系统",
    "概率论与数理统计",
    "数据库原理",
    "计算机网络",
    "马克思主义基本原理",
)

# 高校性质优先级：录取时按分数段匹配，若对应性质无高校则依次降级
NATURE_FALLBACK: dict[str, tuple[str, ...]] = {
    "985": ("985", "211", "一本", "其他"),
    "211": ("211", "一本", "其他"),
    "一本": ("一本", "其他"),
    "其他": ("其他",),
}

# 每线程单次批量写入的行数上限（防止单条 SQL 过大）
_SUB_BATCH = 5000


def _clamp(value: float, lo: float, hi: float) -> float:
    """将数值限制在 [lo, hi] 区间内。

    Args:
        value: 原始数值。
        lo: 下限。
        hi: 上限。

    Returns:
        限幅后的数值。
    """
    return max(lo, min(hi, value))


def _split_id_windows(
    lo: int | None, hi: int | None, window: int
) -> list[tuple[int, int]]:
    """把闭区间 [lo, hi] 按 window 大小切分为多个 ID 窗口。

    Args:
        lo: 区间起点（None 表示无目标数据）。
        hi: 区间终点（None 表示无目标数据）。
        window: 单窗口最大 ID 跨度。

    Returns:
        (窗口起点, 窗口终点) 列表；无数据时返回空列表。
    """
    if lo is None or hi is None:
        return []
    return [(s, min(s + window - 1, hi)) for s in range(lo, hi + 1, window)]


def _score_to_gpa(score: float) -> float:
    """把百分制平均分映射为 4.0 制绩点（常用国内高校标准）。

    Args:
        score: 百分制平均分。

    Returns:
        对应的绩点值（0.0 - 4.0）。
    """
    if score >= 90:
        return 4.0
    if score >= 85:
        return 3.7
    if score >= 82:
        return 3.3
    if score >= 78:
        return 3.0
    if score >= 75:
        return 2.7
    if score >= 72:
        return 2.3
    if score >= 68:
        return 2.0
    if score >= 64:
        return 1.5
    if score >= 60:
        return 1.0
    return 0.0


# ---------------------------------------------------------------------------
# 1. 模拟高考 simu_ncee
# ---------------------------------------------------------------------------
def _ncee_chunk(
    db: SCDBMySQLSpeed, lo: int, hi: int, year: int, exam_date: str
) -> int:
    """为单个 ID 窗口内符合高三年龄段的学生生成高考成绩并入库。

    Args:
        db: 共享连接池句柄。
        lo: 窗口起点。
        hi: 窗口终点。
        year: 高考年份。
        exam_date: 考试日期（当年 6 月 20 日）。

    Returns:
        本窗口写入的成绩行数。
    """
    students = db.fetch_all(
        "SELECT id FROM students"
        " WHERE id BETWEEN %s AND %s AND status = 0"
        " AND YEAR(birthday) BETWEEN %s AND %s",
        (lo, hi, year - 19, year - 18),
        result_format="dict",
    )
    if not students:
        return 0
    rows: list[tuple[Any, ...] | list[Any] | dict[Any, Any]] = [
        (
            s["id"],
            int(
                round(
                    _clamp(
                        random.gauss(GAOKAO_MEAN, GAOKAO_STD),
                        GAOKAO_MIN,
                        GAOKAO_MAX,
                    )
                )
            ),
            exam_date,
        )
        for s in students
    ]
    db.execute_many(
        "INSERT INTO gaokao_scores (student_id, score, exam_date)"
        " VALUES (%s, %s, %s)",
        rows,
    )
    return len(rows)


@app.task(name=TaskName.SIMU_NCEE)
def simu_ncee(
    year: int, chunk_size: int = 50_000, max_workers: int = 8
) -> dict:
    """模拟指定年份的高考。

    选取「入学状态 = 0（未高考）」且出生年份在 ``year-19`` 与
    ``year-18`` 之间（对应高三年龄段）的学生，按正态分布
    （均值 530、标准差 50、区间 [400, 660]）生成高考成绩，
    考试日期固定为当年 6 月 20 日。完成后将这部分学生的
    入学状态更新为 10（已高考未入学）。

    Args:
        year: 高考年份。
        chunk_size: ID 窗口大小（默认 50000）。
        max_workers: 并发线程数（默认 8）。

    Returns:
        执行摘要（simulated：登记成绩人数）。
    """
    started = time.monotonic()
    exam_date = f"{year}-06-20"
    max_workers = max(1, min(max_workers, 32))
    db = SCDBMySQLSpeed(_build_meta(pool_size=max_workers))
    try:
        bounds = db.fetch_all(
            "SELECT MIN(id) AS lo, MAX(id) AS hi FROM students"
            " WHERE status = 0 AND YEAR(birthday) BETWEEN %s AND %s",
            (year - 19, year - 18),
            result_format="dict",
        )[0]
        windows = _split_id_windows(bounds["lo"], bounds["hi"], chunk_size)
        logger.info(
            "[simu_ncee] year={} 高三学生 ID 范围 [{}, {}]，"
            "窗口数={}，线程数={}",
            year,
            bounds["lo"],
            bounds["hi"],
            len(windows),
            max_workers,
        )
        inserted = 0
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = [
                pool.submit(_ncee_chunk, db, w_lo, w_hi, year, exam_date)
                for w_lo, w_hi in windows
            ]
            for future in as_completed(futures):
                inserted += future.result()
        # 状态流转：0（未高考）→ 10（已高考未入学）
        db.execute(
            "UPDATE students SET status = 10"
            " WHERE status = 0 AND YEAR(birthday) BETWEEN %s AND %s",
            (year - 19, year - 18),
        )
        # 年度统计：本年度（exam_date 命中）实际参加高考的学生总数
        total_for_year = db.fetch_all(
            "SELECT COUNT(*) AS c FROM gaokao_scores WHERE exam_date = %s",
            (exam_date,),
            result_format="dict",
        )[0]["c"]
    finally:
        db.close()

    elapsed = round(time.monotonic() - started, 2)
    summary = {
        "task": "simu_ncee",
        "year": year,
        "exam_date": exam_date,
        "simulated": inserted,
        "total_for_year": total_for_year,
        "elapsed_seconds": elapsed,
    }
    logger.info("[simu_ncee] 完成: {}", summary)
    return summary


# ---------------------------------------------------------------------------
# 2. 高校录取 simu_admission
# ---------------------------------------------------------------------------
def _admission_thresholds(
    db: SCDBMySQLSpeed, exam_date: str
) -> dict[str, Any]:
    """基于高考分数直方图计算各录取批次的分数线。

    分数只有数百个离散取值，GROUP BY 得到直方图后按分数从高到低
    累计，即可精确得到前 5% / 15% / 30% 的分数线，
    避免对千万行数据排序。

    Args:
        db: 共享连接池句柄。
        exam_date: 高考考试日期。

    Returns:
        包含 total（考生总数）与 t985 / t211 / t_ben 分数线的字典。
    """
    hist = db.fetch_all(
        "SELECT score, COUNT(*) AS c FROM gaokao_scores"
        " WHERE exam_date = %s GROUP BY score",
        (exam_date,),
        result_format="dict",
    )
    total = sum(row["c"] for row in hist)
    thresholds: dict[str, Any] = {"total": total}
    cumulative = 0
    marks = {"t985": 0.05, "t211": 0.15, "t_ben": 0.30}
    pending = sorted(marks.items(), key=lambda kv: kv[1])
    for score_row in sorted(hist, key=lambda r: r["score"], reverse=True):
        cumulative += score_row["c"]
        while pending and cumulative >= pending[0][1] * total:
            thresholds[pending[0][0]] = score_row["score"]
            pending.pop(0)
    for name, _ in pending:
        thresholds[name] = GAOKAO_MIN  # 考生过少时的兜底
    logger.info(
        "[simu_admission] 考生总数={}，分数线 985/211/一本 = {}/{}/{}",
        total,
        thresholds["t985"],
        thresholds["t211"],
        thresholds["t_ben"],
    )
    return thresholds


def _load_university_pools(
    db: SCDBMySQLSpeed,
) -> dict[str, list[tuple[int, list[int]]]]:
    """按高校性质加载「高校 → 专业组」随机分配池。

    Args:
        db: 共享连接池句柄。

    Returns:
        性质（985/211/一本/其他）到
        ``[(高校 ID, [专业组 ID, ...]), ...]`` 列表的映射。
    """
    universities = db.fetch_all(
        "SELECT id, nature FROM universities", result_format="dict"
    )
    major_groups = db.fetch_all(
        "SELECT id, university_id FROM major_groups", result_format="dict"
    )
    majors_by_uni: dict[int, list[int]] = {}
    for row in major_groups:
        majors_by_uni.setdefault(row["university_id"], []).append(row["id"])
    pools: dict[str, list[tuple[int, list[int]]]] = {
        nature: [] for nature in NATURE_FALLBACK
    }
    for un in universities:
        majors = majors_by_uni.get(un["id"], [])
        if majors:
            pools.setdefault(un["nature"], []).append((un["id"], majors))
    return pools


def _pick_pool(
    pools: dict[str, list[tuple[int, list[int]]]], band: str
) -> tuple[int, list[int]] | None:
    """按分数段对应的性质选取分配池，必要时依次降级。

    Args:
        pools: 性质到分配池的映射。
        band: 分数段对应的性质。

    Returns:
        (高校 ID, 专业组 ID 列表)；所有候选均为空时返回 None。
    """
    for nature in NATURE_FALLBACK[band]:
        if pools.get(nature):
            return random.choice(pools[nature])
    return None


def _admission_chunk(
    db: SCDBMySQLSpeed,
    lo: int,
    hi: int,
    year: int,
    exam_date: str,
    thresholds: dict[str, Any],
    pools: dict[str, list[tuple[int, list[int]]]],
) -> tuple[int, dict[str, int]]:
    """为单个 ID 窗口内已高考的学生执行随机录取并入库。

    Args:
        db: 共享连接池句柄。
        lo: 窗口起点。
        hi: 窗口终点。
        year: 高考年份（同时作为入学学年）。
        exam_date: 高考考试日期。
        thresholds: 各批次分数线。
        pools: 高校随机分配池。

    Returns:
        (录取人数, 各性质录取人数统计)。
    """
    students = db.fetch_all(
        "SELECT s.id AS sid, g.score AS score FROM students s"
        " JOIN gaokao_scores g ON g.student_id = s.id"
        " WHERE s.id BETWEEN %s AND %s AND s.status = 10"
        " AND g.exam_date = %s",
        (lo, hi, exam_date),
        result_format="dict",
    )
    band_stats: dict[str, int] = {}
    rows: list[tuple[Any, ...] | list[Any] | dict[Any, Any]] = []
    for s in students:
        if s["score"] >= thresholds["t985"]:
            band = "985"
        elif s["score"] >= thresholds["t211"]:
            band = "211"
        elif s["score"] >= thresholds["t_ben"]:
            band = "一本"
        else:
            band = "其他"
        picked = _pick_pool(pools, band)
        if picked is None:
            logger.warning(
                "[simu_admission] 无可分配高校，学生跳过: id={}", s["sid"]
            )
            continue
        un_id, major_ids = picked
        rows.append((s["sid"], un_id, random.choice(major_ids), year))
        band_stats[band] = band_stats.get(band, 0) + 1
    for i in range(0, len(rows), _SUB_BATCH):
        db.execute_many(
            "INSERT INTO student_enrollments"
            " (student_id, university_id, major_group_id, enroll_year)"
            " VALUES (%s, %s, %s, %s)",
            rows[i : i + _SUB_BATCH],
        )
    return len(rows), band_stats


@app.task(name=TaskName.SIMU_ADMISSION)
def simu_admission(
    year: int, chunk_size: int = 50_000, max_workers: int = 8
) -> dict:
    """模拟指定高考年份的高校录取。

    以当年高考成绩排名划分批次：985 高校录取前 5%，211 录取
    前 5%-15%，一本录取前 15%-30%，其他高校录取其余学生；
    专业组在各高校名下随机分配。录取结果登记到
    ``student_enrollments`` 表（enroll_year = 高考年份），
    完成后将被录取学生的入学状态更新为 20（在读）。

    Args:
        year: 高考年份。
        chunk_size: ID 窗口大小（默认 50000）。
        max_workers: 并发线程数（默认 8）。

    Returns:
        执行摘要（admitted：录取人数；band_stats：各批次人数）。
    """
    started = time.monotonic()
    exam_date = f"{year}-06-20"
    max_workers = max(1, min(max_workers, 32))
    db = SCDBMySQLSpeed(_build_meta(pool_size=max_workers))
    try:
        thresholds = _admission_thresholds(db, exam_date)
        pools = _load_university_pools(db)
        bounds = db.fetch_all(
            "SELECT MIN(s.id) AS lo, MAX(s.id) AS hi FROM students s"
            " JOIN gaokao_scores g ON g.student_id = s.id"
            " WHERE s.status = 10 AND g.exam_date = %s",
            (exam_date,),
            result_format="dict",
        )[0]
        windows = _split_id_windows(bounds["lo"], bounds["hi"], chunk_size)
        logger.info(
            "[simu_admission] year={} 待录取学生 ID 范围 [{}, {}]，"
            "窗口数={}，线程数={}",
            year,
            bounds["lo"],
            bounds["hi"],
            len(windows),
            max_workers,
        )
        admitted = 0
        band_stats: dict[str, int] = {}
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = [
                pool.submit(
                    _admission_chunk,
                    db,
                    w_lo,
                    w_hi,
                    year,
                    exam_date,
                    thresholds,
                    pools,
                )
                for w_lo, w_hi in windows
            ]
            for future in as_completed(futures):
                count, chunk_stats = future.result()
                admitted += count
                for band, n in chunk_stats.items():
                    band_stats[band] = band_stats.get(band, 0) + n
        # 状态流转：10（已高考未入学）→ 20（在读）
        db.execute(
            "UPDATE students s JOIN gaokao_scores g"
            " ON g.student_id = s.id"
            " SET s.status = 20"
            " WHERE s.status = 10 AND g.exam_date = %s",
            (exam_date,),
        )
        # 年度统计：本年度（enroll_year 命中）实际被录取的学生总数
        total_for_year = db.fetch_all(
            "SELECT COUNT(*) AS c FROM student_enrollments"
            " WHERE enroll_year = %s",
            (year,),
            result_format="dict",
        )[0]["c"]
    finally:
        db.close()

    elapsed = round(time.monotonic() - started, 2)
    summary = {
        "task": "simu_admission",
        "year": year,
        "admitted": admitted,
        "total_for_year": total_for_year,
        "band_stats": band_stats,
        "elapsed_seconds": elapsed,
    }
    logger.info("[simu_admission] 完成: {}", summary)
    return summary


# ---------------------------------------------------------------------------
# 3. 高校日常考试 simu_exam
# ---------------------------------------------------------------------------
def _random_exam_date(year: int) -> str:
    """生成本学年内一个随机考试日期（避开寒暑假月份）。

    学年为 ``year - year+1``：9-12 月属于起始年，3-6 月属于
    次年；寒暑假（1、2、7、8 月）不安排考试。

    Args:
        year: 学年起始年份。

    Returns:
        形如 ``YYYY-MM-DD`` 的考试日期字符串。
    """
    month = random.choice((3, 4, 5, 6, 9, 10, 11, 12))
    exam_year = year if month >= 9 else year + 1
    return f"{exam_year}-{month:02d}-{random.randint(1, 28):02d}"


def _exam_chunk(
    db: SCDBMySQLSpeed, lo: int, hi: int, year: int, academic_year: str
) -> int:
    """为单个 ID 窗口内的在读学生生成日常考试成绩并入库。

    Args:
        db: 共享连接池句柄。
        lo: 窗口起点。
        hi: 窗口终点。
        year: 学年起始年份。
        academic_year: 学年字符串（如 2025-2026）。

    Returns:
        本窗口写入的成绩行数。
    """
    students = db.fetch_all(
        "SELECT id FROM students"
        " WHERE id BETWEEN %s AND %s AND status = 20",
        (lo, hi),
        result_format="dict",
    )
    if not students:
        return 0
    rows: list[tuple[Any, ...] | list[Any] | dict[Any, Any]] = []
    for s in students:
        for _ in range(random.randint(5, 10)):
            rows.append(
                (
                    s["id"],
                    academic_year,
                    random.choice(SUBJECTS),
                    round(
                        _clamp(
                            random.gauss(EXAM_MEAN, EXAM_STD),
                            EXAM_MIN,
                            EXAM_MAX,
                        ),
                        1,
                    ),
                    _random_exam_date(year),
                )
            )
    inserted = 0
    for i in range(0, len(rows), _SUB_BATCH):
        db.execute_many(
            "INSERT INTO undergraduate_scores"
            " (student_id, academic_year, subject, score, exam_date)"
            " VALUES (%s, %s, %s, %s, %s)",
            rows[i : i + _SUB_BATCH],
        )
        inserted += len(rows[i : i + _SUB_BATCH])
    return inserted


@app.task(name=TaskName.SIMU_EXAM)
def simu_exam(
    year: int, chunk_size: int = 50_000, max_workers: int = 8
) -> dict:
    """模拟指定学年的高校日常考试。

    针对「入学状态 = 20（在读）」的学生，每人在学年
    ``year - year+1`` 内随机参加 5-10 次考试；分数按正态分布
    （均值 75、标准差 15）生成并限制在 [0, 100]，考试日期随机
    分布在非寒暑假月份（3-6、9-12 月）。成绩登记到
    ``undergraduate_scores`` 表。

    Args:
        year: 学年起始年份（如 2025 表示 2025-2026 学年）。
        chunk_size: ID 窗口大小（默认 50000）。
        max_workers: 并发线程数（默认 8）。

    Returns:
        执行摘要（recorded：登记成绩行数）。
    """
    started = time.monotonic()
    academic_year = f"{year}-{year + 1}"
    max_workers = max(1, min(max_workers, 32))
    db = SCDBMySQLSpeed(_build_meta(pool_size=max_workers))
    try:
        bounds = db.fetch_all(
            "SELECT MIN(id) AS lo, MAX(id) AS hi FROM students"
            " WHERE status = 20",
            result_format="dict",
        )[0]
        windows = _split_id_windows(bounds["lo"], bounds["hi"], chunk_size)
        logger.info(
            "[simu_exam] 学年={} 在读学生 ID 范围 [{}, {}]，"
            "窗口数={}，线程数={}",
            academic_year,
            bounds["lo"],
            bounds["hi"],
            len(windows),
            max_workers,
        )
        recorded = 0
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = [
                pool.submit(_exam_chunk, db, w_lo, w_hi, year, academic_year)
                for w_lo, w_hi in windows
            ]
            for future in as_completed(futures):
                recorded += future.result()
        # 年度统计：本学年实际举行的日常考试行数与参考学生数
        year_stats = db.fetch_all(
            "SELECT COUNT(*) AS exams,"
            " COUNT(DISTINCT student_id) AS students"
            " FROM undergraduate_scores WHERE academic_year = %s",
            (academic_year,),
            result_format="dict",
        )[0]
    finally:
        db.close()

    elapsed = round(time.monotonic() - started, 2)
    summary = {
        "task": "simu_exam",
        "academic_year": academic_year,
        "recorded": recorded,
        "total_exams_for_year": year_stats["exams"],
        "total_students_for_year": year_stats["students"],
        "elapsed_seconds": elapsed,
    }
    logger.info("[simu_exam] 完成: {}", summary)
    return summary


# ---------------------------------------------------------------------------
# 4. 本科毕业 simu_graduate
# ---------------------------------------------------------------------------
def _graduate_chunk(
    db: SCDBMySQLSpeed, lo: int, hi: int, year: int, graduate_date: str
) -> tuple[int, int]:
    """为单个 ID 窗口内的应届大四学生计算绩点并登记毕业。

    Args:
        db: 共享连接池句柄。
        lo: 窗口起点。
        hi: 窗口终点。
        year: 毕业年份。
        graduate_date: 毕业日期（当年 7 月 1 日）。

    Returns:
        (登记毕业人数, 无本科成绩的人数)。
    """
    students = db.fetch_all(
        "SELECT DISTINCT s.id AS sid FROM students s"
        " JOIN student_enrollments e ON e.student_id = s.id"
        " WHERE s.status = 20 AND e.enroll_year = %s"
        " AND s.id BETWEEN %s AND %s",
        (year - 3, lo, hi),
        result_format="dict",
    )
    if not students:
        return 0, 0
    student_ids = [s["sid"] for s in students]
    placeholders = ", ".join(["%s"] * len(student_ids))
    avg_rows = db.fetch_all(
        "SELECT student_id, AVG(score) AS avg_score"
        f" FROM undergraduate_scores WHERE student_id IN ({placeholders})"
        " GROUP BY student_id",
        tuple(student_ids),
        result_format="dict",
    )
    avg_by_student = {r["student_id"]: r["avg_score"] for r in avg_rows}
    rows: list[tuple[Any, ...] | list[Any] | dict[Any, Any]] = []
    no_score = 0
    for sid in student_ids:
        if sid in avg_by_student:
            gpa = _score_to_gpa(float(avg_by_student[sid]))
        else:
            gpa = 0.0  # 无本科成绩记录，绩点记 0
            no_score += 1
        rows.append((sid, gpa, graduate_date))
    db.execute_many(
        "INSERT INTO graduation_scores (student_id, gpa, graduate_date)"
        " VALUES (%s, %s, %s)",
        rows,
    )
    return len(rows), no_score


@app.task(name=TaskName.SIMU_GRADUATE)
def simu_graduate(
    year: int, chunk_size: int = 50_000, max_workers: int = 8
) -> dict:
    """模拟指定年份的本科毕业。

    针对「入学状态 = 20（在读）」且 ``enroll_year = year - 3``
    （四年制大四年龄段）的学生，基于其本科阶段全部考试成绩的
    平均分映射为 4.0 制绩点，登记到 ``graduation_scores``
    （毕业日期为当年 7 月 1 日），并将入学状态更新为
    30（已毕业）。

    Args:
        year: 毕业年份。
        chunk_size: ID 窗口大小（默认 50000）。
        max_workers: 并发线程数（默认 8）。

    Returns:
        执行摘要（graduated：毕业人数；no_score：无成绩人数）。
    """
    started = time.monotonic()
    graduate_date = f"{year}-07-01"
    max_workers = max(1, min(max_workers, 32))
    db = SCDBMySQLSpeed(_build_meta(pool_size=max_workers))
    try:
        bounds = db.fetch_all(
            "SELECT MIN(s.id) AS lo, MAX(s.id) AS hi FROM students s"
            " JOIN student_enrollments e ON e.student_id = s.id"
            " WHERE s.status = 20 AND e.enroll_year = %s",
            (year - 3,),
            result_format="dict",
        )[0]
        windows = _split_id_windows(bounds["lo"], bounds["hi"], chunk_size)
        logger.info(
            "[simu_graduate] year={} 应届大四学生 ID 范围 [{}, {}]，"
            "窗口数={}，线程数={}",
            year,
            bounds["lo"],
            bounds["hi"],
            len(windows),
            max_workers,
        )
        graduated = 0
        no_score = 0
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = [
                pool.submit(
                    _graduate_chunk, db, w_lo, w_hi, year, graduate_date
                )
                for w_lo, w_hi in windows
            ]
            for future in as_completed(futures):
                count, missing = future.result()
                graduated += count
                no_score += missing
        # 状态流转：20（在读）→ 30（已毕业）
        db.execute(
            "UPDATE students s JOIN student_enrollments e"
            " ON e.student_id = s.id"
            " SET s.status = 30"
            " WHERE s.status = 20 AND e.enroll_year = %s",
            (year - 3,),
        )
        # 年度统计：本年度（graduate_date 命中）实际毕业的学生总数
        total_for_year = db.fetch_all(
            "SELECT COUNT(*) AS c FROM graduation_scores"
            " WHERE graduate_date = %s",
            (graduate_date,),
            result_format="dict",
        )[0]["c"]
    finally:
        db.close()

    elapsed = round(time.monotonic() - started, 2)
    summary = {
        "task": "simu_graduate",
        "year": year,
        "graduate_date": graduate_date,
        "graduated": graduated,
        "total_for_year": total_for_year,
        "no_score": no_score,
        "elapsed_seconds": elapsed,
    }
    logger.info("[simu_graduate] 完成: {}", summary)
    return summary


@app.task(name=TaskName.SIMU_SCHOOL_YEAR)
def simu_school_year(
    year: int,
    stage_timeout: float = 600.0,
    chunk_size: int = 50_000,
    max_workers: int = 8,
) -> dict:
    """模拟学年例行操作：依次执行高考/录取/日常考试/毕业四个任务。

    编排顺序与各阶段的目标年份：

    ======  ==============================  ==========
    阶段    任务                            目标年份
    ======  ==============================  ==========
    高考    tasks.simu_ncee                 year
    录取    tasks.simu_admission            year
    考试    tasks.simu_exam                 year
    毕业    tasks.simu_graduate             year + 3
    ======  ==============================  ==========

    各阶段通过消息队列投递并等待结果，阶段间存在业务依赖
    （录取依赖高考成绩、毕业依赖本科成绩），因此串行执行；
    任一阶段失败即中断编排。

    Args:
        year: 学年起始年份。
        stage_timeout: 单个阶段等待结果的最长秒数（默认 600）。
        chunk_size: 传递给各阶段的 ID 窗口大小（默认 50000）。
        max_workers: 传递给各阶段的并发线程数（默认 8）。

    Returns:
        各阶段执行摘要与年度统计的汇总::

            {"year": 2025,
             "statistics": {"gaokao_students": ...,
                            "admitted_students": ...,
                            "exam_records": ...,
                            "exam_students": ...,
                            "graduated_students": ...},
             "stages": {"ncee": {...}, "admission": {...},
                        "exam": {...}, "graduate": {...}},
             "elapsed_seconds": 45.2}
    """
    started = time.monotonic()
    # 入参经契约 Schema 校验
    payload = SimuSchoolYearPayload(
        year=year,
        stage_timeout=stage_timeout,
        chunk_size=chunk_size,
        max_workers=max_workers,
    )
    logger.info(
        "[simu_school_year] 开始学年例行操作: year={}, "
        "stage_timeout={}, chunk_size={}, max_workers={}",
        payload.year,
        payload.stage_timeout,
        payload.chunk_size,
        payload.max_workers,
    )

    # 阶段定义：(阶段名, 任务函数, 目标年份)
    stages = (
        ("ncee", simu_ncee, payload.year),
        ("admission", simu_admission, payload.year),
        ("exam", simu_exam, payload.year),
        ("graduate", simu_graduate, payload.year + 3),
    )

    stage_summaries: dict[str, dict] = {}
    db = SCDBMySQLSpeed(_build_meta())
    try:
        for stage_name, task_fn, stage_year in stages:
            logger.info(
                "[simu_school_year] 阶段 [{}] 开始: task={}, year={}",
                stage_name,
                task_fn.name,
                stage_year,
            )
            # 编排任务内同步等待子任务：Celery 默认禁止在任务内调用
            # result.get()（防死锁），这里显式关闭该限制——本 worker 使用
            # threads 池且 concurrency=4，编排任务仅占用 1 个线程，
            # 子任务仍有剩余线程可用，不存在死锁风险
            stage_summaries[stage_name] = task_fn.delay(
                year=stage_year,
                chunk_size=payload.chunk_size,
                max_workers=payload.max_workers,
            ).get(timeout=payload.stage_timeout, disable_sync_subtasks=False)
            logger.info(
                "[simu_school_year] 阶段 [{}] 完成: {}",
                stage_name,
                stage_summaries[stage_name],
            )
    finally:
        db.close()

    elapsed = round(time.monotonic() - started, 2)

    # 年度统计汇总（取自各阶段摘要中基于数据库的年度计数）
    statistics = {
        # 本年度参加高考的学生数量
        "gaokao_students": stage_summaries["ncee"]["total_for_year"],
        # 本年度被高校录取的学生数量
        "admitted_students": stage_summaries["admission"]["total_for_year"],
        # 本学年举行的日常考试次数（成绩行数）
        "exam_records": stage_summaries["exam"]["total_exams_for_year"],
        # 本学年参加日常考试的学生数量
        "exam_students": stage_summaries["exam"]["total_students_for_year"],
        # 本年度毕业的学生数量
        "graduated_students": stage_summaries["graduate"]["total_for_year"],
    }

    summary = {
        "year": payload.year,
        "statistics": statistics,
        "stages": stage_summaries,
        "elapsed_seconds": elapsed,
    }
    logger.info("[simu_school_year] 全部阶段完成: {}", summary)
    return summary
