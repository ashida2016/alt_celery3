"""Celery 应用实例与全局配置模块。

从环境变量读取 broker / backend 等必要参数，集中定义 beat_schedule
定时任务调度表，并通过 autodiscover 自动发现 app/tasks 子包中的任务
模块。后续新增任务只需在 tasks/ 目录下新建 py 文件，无需修改本文件。
"""

import os

from celery import Celery
from celery.schedules import crontab

# ---------------------------------------------------------------------------
# 环境变量配置
# ---------------------------------------------------------------------------
# 消息中间件（外部已有带密码的 redis-stack），例如:
#   redis://:password@redis-stack-host:6379/0
BROKER_URL: str = os.environ.get(
    "CELERY_BROKER_URL", "redis://localhost:6379/0"
)
# 结果后端，与 broker 通常共用同一 redis-stack，例如:
#   redis://:password@redis-stack-host:6379/1
RESULT_BACKEND: str = os.environ.get(
    "CELERY_RESULT_BACKEND", "redis://localhost:6379/1"
)

app = Celery(
    "alt_celery3",
    broker=BROKER_URL,
    backend=RESULT_BACKEND,
    include=["app.tasks.math_tasks", "app.tasks.db_tasks"],
)

# ---------------------------------------------------------------------------
# 生产级配置
# ---------------------------------------------------------------------------
app.conf.update(
    # 序列化：统一使用 JSON，禁止 pickle 以保证安全
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    # 时区：定时任务按上海时间调度
    timezone="Asia/Shanghai",
    enable_utc=False,
    # 结果过期时间（秒），避免 redis backend 无限膨胀
    result_expires=3600,
    # worker 行为：prefetch 设为 1 保证任务分配均匀，长任务不饿死短任务
    worker_prefetch_multiplier=1,
    task_acks_late=True,
    worker_max_tasks_per_child=1000,
    # 任务路由：默认队列 + 数据库任务专用队列
    # （db 队列与 default 隔离，避免被只监听 default 的旧代码 worker 抢占）
    task_default_queue="default",
    task_routes={
        "tasks.try_mysql": {"queue": "db"},
        "tasks.get_one_student": {"queue": "db"},
    },
    # broker 连接可靠性
    broker_connection_retry_on_startup=True,
    # 任务执行超时保护
    task_soft_time_limit=300,
    task_time_limit=600,
    # 日志输出到 stdout，便于 docker logs 收集
    worker_hijack_root_logger=False,
)

# ---------------------------------------------------------------------------
# 定时任务调度表（beat_schedule）
# ---------------------------------------------------------------------------
app.conf.beat_schedule = {
    # 示例定时任务：每分钟执行一次周期加法
    "periodic-add-every-minute": {
        "task": "tasks.periodic_add",
        "schedule": crontab(minute="*/1"),
        "args": (1, 2),
        "options": {"queue": "default"},
    },
}

# 自动发现 app/tasks 子包中的任务模块
app.autodiscover_tasks(["app.tasks"])
