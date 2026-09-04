"""alt_celery3 应用包。

导出 Celery 应用实例，供 worker、beat、flower 以及生产者脚本
（run_tasks.py）统一导入使用。导入前自动加载项目根目录的 ``.env``
文件，保证 ``CELERY_BROKER_URL`` 等变量在任何入口下都生效。
"""

import os

_ENV_FILE = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"
)


def load_dotenv(path: str = _ENV_FILE) -> None:
    """加载 .env 文件中的环境变量（不覆盖已存在的变量）。

    使用简单的 KEY=VALUE 解析，无需额外依赖；文件不存在时静默跳过。

    Args:
        path: .env 文件路径，默认为项目根目录下的 ``.env``。
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


# 必须在导入 celery_app 之前加载 .env，因为 celery_app 在导入时读取环境变量
load_dotenv()

from app.celery_app import app  # noqa: E402

__all__ = ["app", "load_dotenv"]

