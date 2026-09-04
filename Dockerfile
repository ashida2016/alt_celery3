# syntax=docker/dockerfile:1.7
# =============================================================================
# alt_celery3 生产级镜像
# - 基础镜像 python:3.13-slim（满足 Python >= 3.13 约束）
# - 显式创建专用非特权用户 celeuser，celery 进程以该用户运行
# - 依赖层单独缓存，代码变更时不重装依赖
# =============================================================================

# -----------------------------------------------------------------------------
# 阶段 1：构建依赖 wheels
# -----------------------------------------------------------------------------
FROM python:3.13-slim AS builder

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /build

COPY requirements.txt .

# scdb_mysql_speed 依赖 mysqlclient（C 扩展），构建期需要 MySQL 开发头文件与编译工具
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential pkg-config default-libmysqlclient-dev git \
    && rm -rf /var/lib/apt/lists/*

# 先编译 wheels 供后续阶段离线安装，减少最终镜像体积
RUN pip wheel --wheel-dir /wheels -r requirements.txt

# -----------------------------------------------------------------------------
# 阶段 2：运行时镜像
# -----------------------------------------------------------------------------
FROM python:3.13-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app \
    PIP_NO_CACHE_DIR=1

# 显式创建专用非特权用户 celeuser（专供 celery 使用），UID/GUID 固定便于权限管理
RUN apt-get update \
    && apt-get install -y --no-install-recommends libmariadb3 \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 1000 celeuser \
    && useradd --uid 1000 --gid celeuser --create-home --shell /bin/bash celeuser

WORKDIR /app

# 安装依赖（使用 builder 阶段的 wheels）
COPY --from=builder /wheels /wheels
RUN pip install --no-cache-dir --no-index --find-links=/wheels /wheels/* \
    && rm -rf /wheels

# 复制应用代码并调整属主
COPY --chown=celeuser:celeuser app/ ./app/
COPY --chown=celeuser:celeuser run_tasks.py ./

# 切换到非特权用户运行
USER celeuser

# worker 存活探针：通过 ping 检查 celery 应用可用性
HEALTHCHECK --interval=30s --timeout=10s --start-period=20s --retries=3 \
    CMD python -c "import sys; from app import app; sys.exit(0 if app.control.inspect(timeout=5).ping() else 1)" || exit 1

# 默认命令：celery worker（compose 中按服务覆盖 command）
CMD ["celery", "-A", "app.celery_app", "worker", "--loglevel=info"]
