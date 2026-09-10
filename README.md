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
│   ├── log_setup.py         # sclog 日志中间件初始化（控制台/文件 + MySQL sink）
│   └── tasks/               # 任务子文件夹（新增任务放这里）
│       ├── math_tasks.py    # 示例：加法任务 add + 定时任务 periodic_add
│       ├── db_tasks.py      # 数据库任务：try_mysql + get_one_student + generate_many_students
│       ├── simu_tasks.py    # 业务模拟任务：高考/录取/日常考试/毕业（多线程）
│       ├── un_tasks.py      # 高校信息任务：get_un_groups（硅基流动 API + 查重入库）
│       └── init_tasks.py    # 数据库初始化任务：init_web_db（重建库/用户/业务表）
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

`run_tasks.py` 会依次：调用普通加法任务并等待结果、手动触发一次定时任务并获取结果、从 redis backend 列出最近的任务执行结果（含 beat 周期触发的定时任务记录）。Redis 连接参数自动从项目根目录的 `.env` 文件加载（`run_celery.py` 与 celery 命令行同理），无需手动 export。

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

### 3. 数据库任务

`app/tasks/db_tasks.py` 提供两个基于 `scdb_mysql_speed` 的 MySQL 任务，均使用 `sclog` 记录操作日志（持久化到 `log_db`）：

| 任务                    | 说明                                  |
| ----------------------- | -------------------------------------- |
| `tasks.try_mysql`       | 测试业务库（web_db）连通性             |
| `tasks.get_one_student` | 按主键查询单个学生信息（students 表）  |

```python
from app.tasks.db_tasks import try_mysql, get_one_student

print(try_mysql.delay().get(timeout=30))
# {'ok': True, 'database': 'web_db'}
print(get_one_student.delay(1).get(timeout=30))
# {'found': True, 'student': {'id': 1, 'name': '李骧渊', 'gender': 'M', 'birthday': ...}}
```

`students` 表结构（如业务库中不存在，需先创建）：

```sql
CREATE TABLE IF NOT EXISTS students (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(32) NOT NULL,
    gender VARCHAR(4) NOT NULL,
    birthday DATE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

可使用 `alt_generate_zh_name` 包生成随机学生数据灌入该表用于测试。

### 批量生成学生任务 generate_many_students

`tasks.generate_many_students` 批量生成随机学生信息并直接写入 `web_db.students` 表。面向**百万级**数据量做了多线程优化：总量按块切分，线程池并发执行"生成 + 批量入库"，共享数据库连接池。

| 参数           | 类型 | 默认值       | 说明                                     |
| -------------- | ---- | ------------ | ---------------------------------------- |
| `numbers`      | int  | 必填         | 要生成的学生总人数                       |
| `birthday_min` | str  | `2000-01-01` | 出生年月日最小值（YYYY-MM-DD）           |
| `birthday_max` | str  | `2010-12-31` | 出生年月日最大值（YYYY-MM-DD）           |
| `chunk_size`   | int  | `50000`      | 单块人数上限（分块生成与入库）           |
| `max_workers`  | int  | `8`          | 并发线程数（连接池同步扩容，上限 32）    |

通过 `run_tasks.py` 单独指定运行（大批量时请同步调大 `--timeout`）：

```bash
# 生成 10000 条
python run_tasks.py --task generate --numbers 10000 --timeout 120

# 百万级示例（实测约 2 万行/秒，100 万条约 50 秒）
python run_tasks.py --task generate --numbers 1000000 \
    --birthday-min 2000-01-01 --birthday-max 2010-12-31 \
    --chunk-size 50000 --max-workers 8 --timeout 600
```

返回结果示例：

```
{'inserted': 200000, 'numbers': 200000, 'chunk_size': 25000, 'max_workers': 8,
 'elapsed_seconds': 9.6, 'rows_per_second': 20833.9}
```

> 该任务执行耗时较长，已单独设置任务超时上限（soft 1800s / hard 1900s），不受全局 `task_time_limit=600` 限制。

### 高校信息任务 get_un_groups

`tasks.get_un_groups` 通过硅基流动（SiliconFlow）Chat Completion API 自动获取指定数量的高校信息（含下属专业组），以名称查重（重复不添加）后写入 `web_db` 的 `universities` / `major_groups` 表，并返回标准 JSON 对象。

- 公用 API 函数：`app/gjld_api.py` 的 `gjld_chat_completion(question)`，可输入任意问题获得文本回答
- 环境变量：`API_KEY_GJLD`（API-KEY）、`BASE_URL`（默认 `https://api.siliconflow.cn/v1`）、`GJLD_MODEL`（默认 `Qwen/Qwen2.5-72B-Instruct`）
- 数据库表已预先存在（结构由项目方维护），任务不做建表操作
- 该任务路由到 `llm` 专用队列，worker 启动参数需包含 `-Q default,db,llm`

```bash
# 获取 3 所高校信息
python run_tasks.py --task un --count 3 --timeout 300
```

返回 JSON 示例：

```json
[
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
]
```

数据库表结构（已预先存在，任务不建表）：

- `universities`：name（唯一）、code（char(5) 院校标识码，全局唯一）、type（民办/公办）、nature（985/211/一本/其他）
- `major_groups`：university_id（外键）、name、code（char(5)）；同校内 (university_id, code) 唯一

> 查重规则：以名称为准——高校重复不添加，但仍会检查其名下专业组并补录未出现过的专业组；专业组同样以名称查重，重复不添加。同时兼容表的唯一约束，code 冲突的条目记日志后跳过。

### 高校业务模拟任务（ncee / admission / exam / graduate）

覆盖学生「高考 → 录取 → 在读考试 → 毕业」全生命周期的四个模拟任务，均采用多线程 ID 窗口分块处理，面向千万级学生数据优化：

| 任务                | 说明                                                                                                          |
| ------------------- | ------------------------------------------------------------------------------------------------------------- |
| `simu_ncee`         | 模拟高考：选取高三年龄段（出生年在 year-19 ~ year-18）的未高考学生，按正态分布（均值 530、标准差 50、区间 400-660）生成成绩，考试日期固定 6 月 20 日；完成后状态 0→10 |
| `simu_admission`    | 高校录取：按当年高考成绩排名分批——985 录前 5%、211 录 5-15%、一本录 15-30%、其他录剩余；专业组随机分配，登记入学关系表；完成后状态 10→20 |
| `simu_exam`         | 日常考试：仅针对在读学生，每学年每人 5-10 次考试，成绩正态分布（均值 75、标准差 15、区间 0-100），日期避开寒暑假（3-6、9-12 月） |
| `simu_graduate`     | 本科毕业：针对四年前入学的大四学生，按本科全部成绩均值映射 4.0 制绩点（毕业日期 7 月 1 日）；完成后状态 20→30 |

通过 `run_tasks.py` 单独运行（均需 `--year` 指定年份，可调 `--chunk-size` / `--max-workers` / `--timeout`）：

```bash
python run_tasks.py --task ncee      --year 2025 --timeout 300
python run_tasks.py --task admission --year 2025 --timeout 300
python run_tasks.py --task exam      --year 2025 --timeout 600
python run_tasks.py --task graduate  --year 2028 --timeout 300
```

实测吞吐（8 线程）：20 万人高考 6.1s、录取 6.1s、毕业 11.4s；151 万行本科成绩 15.7s（约 9.7 万行/秒）。

### 数据库初始化任务 init_web_db

`tasks.init_web_db` 一键重建 `web_db` / `log_db` 数据库与用户 `web_user` / `log_user`，并在 `web_db` 内创建全部业务表。

**危险操作**：会删除旧库、旧用户及全部数据，必须追加 `--yes` 确认，且需要管理员账号（`.env` 中 `MYSQL_ADMIN_USER` / `MYSQL_ADMIN_PASSWORD`，需全局 DROP/CREATE 权限）。执行后需重启 worker/beat 以恢复 sclog 日志持久化连接。

```bash
python run_tasks.py --task initdb --yes --timeout 120
```

业务表（面向千万级学生数据优化：InnoDB + BIGINT 主键 + 高频查询二级索引；大表不建外键，引用完整性由应用层保证）：

| 表名                   | 说明                                       |
| ---------------------- | ------------------------------------------ |
| `students`             | 学生信息表（索引：name、birthday；含入学状态 status：0=未高考 / 10=已高考未入学 / 20=在读 / 30=已毕业，生成学生默认 0） |
| `universities`         | 高校信息表（name、code 唯一）              |
| `major_groups`         | 专业组信息表（外键关联高校，级联删除）     |
| `gaokao_scores`        | 高考成绩表（student_id 唯一，一人一条）    |
| `undergraduate_scores` | 本科成绩表（学年 + 课程，按学生索引）      |
| `graduation_scores`    | 毕业成绩表（student_id 唯一，含绩点）      |
| `student_enrollments`  | 学生-高校-专业组入学关系表（防重复入学）   |

同时会在 `log_db` 内重建 `app_logs` 日志表，恢复 sclog 日志持久化。
> 注意：`major_groups.code` 为 char(5)，模型返回的 6 位专业代码入库时会截断为前 5 位（返回 JSON 保留原始代码）。

### 4. 新增任务

在 `app/tasks/` 下新建 py 文件（如 `app/tasks/notice_tasks.py`）：

```python
from app.celery_app import app

@app.task(name="tasks.send_notice")
def send_notice(user_id: int, content: str) -> str:
    """发送通知。"""
    return f"notice sent to {user_id}"
```

并在 `app/celery_app.py` 的 `include` 列表中加入 `"app.tasks.notice_tasks"`；如需定时执行，在 `beat_schedule` 中增加条目即可。

### 5. 更新部署

服务器上拉取最新代码并重建拉起服务：

```bash
./update.sh
```

> **注意**：更新代码后务必重建并重启所有 worker。若同一 broker 上存在运行旧代码的其他 worker 节点，新任务可能被旧节点抢占而报 `NotRegistered`。可用 `celery -A app.celery_app inspect ping` 检查在线节点。

> **队列说明**：worker 同时监听 `default`、`db` 与 `llm` 三个队列；数据库任务路由到 `db` 队列，LLM 任务路由到 `llm` 队列（定义于 `app/celery_app.py` 的 `task_routes`），与默认队列隔离。
>
> **进程池说明**：worker 使用 `--pool=threads`（而非默认 prefork）。原因：sclog 的 MySQL sink 依赖主进程内的后台写库线程，prefork 的子进程不继承该线程，会导致任务执行日志无法落库；threads 池的任务在同进程内执行，规避此问题。注意 threads 池下 celery 的时间限制（time_limit）不生效。

## 环境变量说明

| 变量名                | 必填 | 说明                                            | 示例                                          |
| --------------------- | ---- | ----------------------------------------------- | --------------------------------------------- |
| `CELERY_BROKER_URL`   | 是   | 消息中间件连接地址（外部带密码 redis-stack）    | `redis://:pass@redis-stack-host:6379/0`       |
| `CELERY_RESULT_BACKEND` | 是 | 结果后端连接地址（与 broker 共用 redis-stack）  | `redis://:pass@redis-stack-host:6379/1`       |
| `FLOWER_PORT`         | 否   | Flower 对外暴露端口（默认 5555）                | `5555`                                        |
| `APP_DB_HOST`         | 是   | MySQL 业务库地址（web_db，数据库任务使用）      | `192.168.1.101`                               |
| `APP_DB_PORT`         | 否   | MySQL 业务库端口（默认 3306）                   | `3306`                                        |
| `APP_DB_USER`         | 是   | MySQL 业务库用户                                | `web_user`                                    |
| `APP_DB_PASSWORD`     | 是   | MySQL 业务库密码                                | `******`                                      |
| `APP_DB_NAME`         | 否   | MySQL 业务库名（默认 web_db）                   | `web_db`                                      |
| `LOG_DB_HOST`         | 是   | sclog 日志库地址（log_db，日志 sink 使用）      | `192.168.1.101`                               |
| `LOG_DB_PORT`         | 否   | sclog 日志库端口（默认 3306）                   | `3306`                                        |
| `LOG_DB_USER`         | 是   | sclog 日志库用户                                | `log_user`                                    |
| `LOG_DB_PASSWORD`     | 是   | sclog 日志库密码                                | `******`                                      |
| `LOG_DB_NAME`         | 否   | sclog 日志库名（默认 log_db）                   | `log_db`                                      |
| `LOG_DIR`             | 否   | 本地日志文件目录（默认 logs）                   | `logs`                                        |
| `API_KEY_GJLD`        | 是*  | 硅基流动 API-KEY（get_un_groups 任务使用）      | `sk-xxxx`                                     |
| `BASE_URL`            | 否   | 硅基流动 API 地址（默认官方地址）               | `https://api.siliconflow.cn/v1`               |
| `GJLD_MODEL`          | 否   | 硅基流动模型名（默认 Qwen2.5-72B-Instruct）     | `Qwen/Qwen2.5-72B-Instruct`                   |
| `MYSQL_ADMIN_USER`       | 是*  | MySQL 管理员账号（initdb 任务使用）             | `root`                                        |
| `MYSQL_ADMIN_PASSWORD`   | 是*  | MySQL 管理员密码                                | `******`                                      |

\* 使用 `--task initdb` 时必填。

\* 使用 `--task un` 时必填。

## 跨服务任务契约包

本项目配套独立的契约包 [`alt_celery3_contract`](../alt_celery3_contract)（平级目录），静态提炼全部 11 个 Celery 任务的注册名、强类型入参 Schema（Pydantic v2）与契约函数，供其他服务在不依赖 Celery/业务实现的情况下类型化地调用任务：

```python
from alt_celery3_contract import TaskName, schemas

# 跨服务投递时引用枚举任务名，并用 Schema 校验入参
payload = schemas.GenerateManyStudentsPayload(numbers=1_000_000)
app.send_task(TaskName.GENERATE_MANY_STUDENTS.value, kwargs=payload.model_dump())
```

- 本项目的任务注册名与 `task_routes` 均从契约包 `TaskName` 枚举引用（单一事实来源）
- 契约包已作为依赖发布在 GitHub（`pyproject.toml` 固定 `ver0.1.0` 标签），`pip install -e .` 会自动从 GitHub 拉取安装

- 兼容性验证：在契约包目录执行 `python verify_contracts.py`，可静态比对契约与源任务签名（参数名/顺序/默认值/类型注解/Schema 字段）是否漂移

## 本地开发

```bash
# Python >= 3.13
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 配置环境变量（复制示例并填入真实 Redis 连接信息）
cp .env.example .env
```

### 使用 run_celery.py 一键启动

无需 Docker，直接在本地拉起 worker 与 beat（自动加载 `.env` 文件，默认不启动 flower）：

```bash
python run_celery.py                            # 启动 worker + beat
python run_celery.py --components worker        # 只启动 worker
python run_celery.py --components worker,beat,flower  # 额外启动 flower
python run_celery.py --flower-port 5566         # 指定 flower 端口
python run_celery.py --loglevel debug           # 调试日志
```

启动后按 `Ctrl+C` 优雅停止全部进程；如需 flower 监控面板（默认 `http://localhost:5555`），通过 `--components` 显式加入。
nohup python run_celery.py

### 手动分进程启动（等价方式）

```bash
celery -A app.celery_app worker --loglevel=info
celery -A app.celery_app beat --loglevel=info
celery -A app.celery_app flower --port=5555
```
