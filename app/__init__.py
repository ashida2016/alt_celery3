# -*- coding: utf-8 -*-
"""alt_celery3 应用包。

导出 Celery 应用实例，供 worker、beat、flower 以及生产者脚本
（run_tasks.py）统一导入使用。
"""

from app.celery_app import app

__all__ = ["app"]
