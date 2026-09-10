"""数学类示例任务模块。

提供简单加法普通任务（add）与被 beat 周期调度的定时任务
（periodic_add）示例，后续更多任务可参照本文件新建模块。
"""

from alt_celery3_contract.constants import TaskName
from sclog_lite import logger

from app.celery_app import app
from app.log_setup import init_logging

# 确保任务进程内日志中间件已初始化（幂等）
init_logging()


@app.task(name=TaskName.ADD, bind=True, max_retries=3)
def add(self, x: int, y: int) -> int:
    """计算两数之和（普通任务示例）。

    Args:
        self: Celery 任务实例（bind=True 时自动注入）。
        x: 加数。
        y: 被加数。

    Returns:
        两数之和。
    """
    result = x + y
    logger.info("add({}, {}) = {}", x, y, result)
    return result


@app.task(name=TaskName.PERIODIC_ADD)
def periodic_add(x: int, y: int) -> int:
    """周期性加法定时任务示例，由 celery beat 调度。

    Args:
        x: 加数。
        y: 被加数。

    Returns:
        两数之和，结果写入 result backend 供 run_tasks.py 查询。
    """
    result = x + y
    logger.info("periodic_add({}, {}) = {}", x, y, result)
    return result
