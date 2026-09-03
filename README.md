# alt_celery3

一个基于 **Docker + Celery** 的生产级任务平台，可支持高校学生管理系统的后台任务处理。

## 功能简介

- **普通任务**：由生产者（如 `run_tasks.py` 或业务代码）异步下发，worker 执行并返回结果
- **定时任务**：由 celery beat 按调度表周期触发（调度表定义于 `app/celery_app.py` 的 `beat_schedule`）
- **任务扩展**：所有任务主体 py 文件统一放在 `app/tasks/` 子文件夹，通过 `autodiscover_tasks` 自动加载，新增任务零侵入
- **监控面板**：内置 Flower，可在浏览器中查看任务执行状态、worker 负载等
- **安全部署**：容器内以专用非特权用户 `celeuser` 运行，Redis 连接信息经环境变量注入，不落盘

### 架构

```
run_tasks.py(生产者) ──下发任务──▶ redis-stack(broker/result) ──▶ Celery Worker
                                        ▲                          │
                    Celery Beat(定时调度) ┘                          ▼
                                        Flower(监控)          结果写回 redis
```

### 项目结构

```
alt_celery3/
├── app/
│   ├── celery_app.py        # Celery 应用实例、环境变量配置、beat_schedule
│   └── tasks/               # 任务子文件夹（新增任务放这里）
│       └── math_tasks.py    # 示例：加法任务 add + 定时任务 periodic_add
├── run_tasks.py             # 生产者脚本：调用示例任务、获取任务结果
├── run_celery.py            # 本地一键启动 worker / beat / flower
├── Dockerfile               # 多阶段构建，创建 celeuser 非 root 用户
├── docker-compose.yml       # worker / beat / flower 三服务编排
├── requirements.txt         # 传统依赖清单
├── pyproject.toml           # 现代依赖管理（Python >= 3.13）
├── update.sh                # 拉取最新代码并重建拉起 Docker 服务
├── .env.example             # 环境变量示例（复制为 .env 使用）
└── .env                     # 本地环境变量（含示例值，已被 gitignore 排除）
```

## 部署过程

### 前置要求

- Docker 20.10+ 与 Docker Compose v2
- 一个外部已有、带密码保护的 **redis-stack** 服务器（作为 broker 与 result backend）

### 步骤

1. **克隆代码**

   ```bash
   git clone <仓库地址>
   cd alt_celery3
   ```

2. **配置环境变量**

   ```bash
   cp .env.example .env
   vim .env
   ```

   编辑 `.env`，填入 Redis 连接信息：

   ```dotenv
   CELERY_BROKER_URL=redis://:your-redis-password@redis-stack-host:6379/0
   CELERY_RESULT_BACKEND=redis://:your-redis-password@redis-stack-host:6379/1
   FLOWER_PORT=5555
   ```

3. **构建并启动服务**

   ```bash
   docker compose up -d --build
   ```

   启动后包含三个服务：

   | 服务     | 说明                         | 端口 |
   | -------- | ---------------------------- | ---- |
   | worker   | 执行任务                     | -    |
   | beat     | 定时任务调度（单实例）       | -    |
   | flower   | 任务监控面板                 | 5555 |

4. **验证服务状态**

   ```bash
   docker compose ps
   docker compose logs -f worker
   ```

5. **访问 Flower 监控面板**

   浏览器打开 `http://<宿主机IP>:5555`。

## 使用示例

### 1. 运行示例脚本 run_tasks.py

`run_tasks.py` 会依次：调用普通加法任务并等待结果、手动触发一次定时任务并获取结果、从 redis backend 列出最近的任务执行结果（含 beat 周期触发的定时任务记录）。

```bash
# 宿主机运行（需 Python >= 3.13 并安装依赖）
pip install -r requirements.txt
python run_tasks.py

# 或进入 worker 容器内运行
docker compose exec worker python run_tasks.py
```

输出示例：

```
[普通任务] tasks.add(2, 3) = 5
[定时任务] tasks.periodic_add(10, 20) = 30

[最近任务结果] (来自 result backend):
  task_id=xxxx status=SUCCESS result=30
  ...
```

### 2. 在业务代码中作为生产者下发任务

```python
from app.tasks.math_tasks import add, periodic_add

# 普通任务：异步下发
result = add.delay(1, 2)
print(result.get(timeout=30))  # 3

# 定时任务由 beat 周期触发，也可手动下发一次
periodic_result = periodic_add.delay(4, 5)
print(periodic_result.get(timeout=30))  # 9
```

### 3. 新增任务

在 `app/tasks/` 下新建 py 文件（如 `app/tasks/notice_tasks.py`）：

```python
from app.celery_app import app

@app.task(name="tasks.send_notice")
def send_notice(user_id: int, content: str) -> str:
    """发送通知。"""
    return f"notice sent to {user_id}"
```

并在 `app/celery_app.py` 的 `include` 列表中加入 `"app.tasks.notice_tasks"`；如需定时执行，在 `beat_schedule` 中增加条目即可。

### 4. 更新部署

服务器上拉取最新代码并重建拉起服务：

```bash
./update.sh
```

## 环境变量说明

| 变量名                | 必填 | 说明                                            | 示例                                          |
| --------------------- | ---- | ----------------------------------------------- | --------------------------------------------- |
| `CELERY_BROKER_URL`   | 是   | 消息中间件连接地址（外部带密码 redis-stack）    | `redis://:pass@redis-stack-host:6379/0`       |
| `CELERY_RESULT_BACKEND` | 是 | 结果后端连接地址（与 broker 共用 redis-stack）  | `redis://:pass@redis-stack-host:6379/1`       |
| `FLOWER_PORT`         | 否   | Flower 对外暴露端口（默认 5555）                | `5555`                                        |

## 本地开发

```bash
# Python >= 3.13
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 配置环境变量（复制示例并填入真实 Redis 连接信息）
cp .env.example .env
```

### 使用 run_celery.py 一键启动

无需 Docker，直接在本地拉起全套服务（自动加载 `.env` 文件）：

```bash
python run_celery.py                            # 启动 worker + beat + flower
python run_celery.py --components worker,beat   # 只启动部分组件
python run_celery.py --flower-port 5566         # 指定 flower 端口
python run_celery.py --loglevel debug           # 调试日志
```

启动后按 `Ctrl+C` 优雅停止全部进程；Flower 面板默认在 `http://localhost:5555`。

### 手动分进程启动（等价方式）

```bash
celery -A app.celery_app worker --loglevel=info
celery -A app.celery_app beat --loglevel=info
celery -A app.celery_app flower --port=5555
```
