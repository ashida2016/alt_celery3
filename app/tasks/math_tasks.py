# -*- coding: utf-8 -*-
"""数学类示例任务模块。

提供简单加法普通任务（add）与被 beat 周期调度的定时任务
（periodic_add）示例，后续更多任务可参照本文件新建模块。
"""

import logging

from app.celery_app import app

logger = logging.getLogger(__name__)


@app.task(name="tasks.add", bind=True, max_retries=3)
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
    logger.info("add(%s, %s) = %s", x, y, result)
    return result


@app.task(name="tasks.periodic_add")
def periodic_add(x: int, y: int) -> int:
    """周期性加法定时任务示例，由 celery beat 调度。

    Args:
        x: 加数。
        y: 被加数。

    Returns:
        两数之和，结果写入 result backend 供 run_tasks.py 查询。
    """
    result = x + y
    logger.info("periodic_add(%s, %s) = %s", x, y, result)
    return result
