# ai-task-orchestrator

轻量级任务编排与运行管理平台。将任意可执行程序打包为 task，拖拽编排为 pipeline，串行执行并自动管理数据传递。

**与 AI 解耦** — task 可以是任意可执行程序，平台不感知内部实现。**单二进制部署** — Go 编译，零运行时依赖。

## 设计原则

- **与 AI/大模型解耦**：ai-task-orchestrator 是通用任务编排与进程管理平台，不感知 task 内部是否是 AI agent。task 可以是任意可执行程序（调用大模型 API 的脚本、数据处理程序、模型训练任务等）。
- **舞台与演员分离**：ai-task-orchestrator 提供"舞台和调度"，task 是"演员"。舞台不需要知道演员在演什么。

## 快速开始

```bash
# 构建
go build -o ai-task-orchestrator .

# 启动（web 资源已内嵌到二进制，可从任意目录启动）
./ai-task-orchestrator

# 自定义配置
./ai-task-orchestrator -data /var/lib/orchestrator -port 9090 -log-level debug
```

启动参数：

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `-port` | `8080` | HTTP 监听端口 |
| `-data` | `./data` | 数据目录 |
| `-log-level` | `info` | 日志级别: debug / info / warn / error |

## 核心概念

| 概念 | 说明 |
|---|---|
| **Task** | 用户上传的 tar 包。Task name 取自文件名，全局唯一，不可改名。可通过 `for-task-orchestrator.txt` 自描述 run/stop 命令，否则默认 `./run.sh` / `./stop.sh`。支持下载导出。 |
| **Pipeline** | 多个 task 的有序编排，严格串行执行。支持同一 task 在同一 pipeline 中多次出现（如 taskA → taskA → taskB → taskB），每个出现称为一个 task 实例，由其在数组中的索引唯一标识。 |
| **Run** | Pipeline 的一次实际执行，包含每个 task 的运行状态、日志和数据。 |

### 全局约束

- **多流水线并发**：不同流水线可以同时运行，互不干扰，每条流水线有独立的数据目录和运行控制。
- **严格串行**：单条流水线内 task 逐个执行，不支持并行。想并发可自行在 task 内实现。
- **停止 = 失败**：手动停止流水线 → stop_command 执行 → 超时 10s 后 SIGKILL 强杀 → 标记当前 task failed → 链上后续不触发。
- **上游数据自理**：第一个 task 的输入由 task 自身解决，ai-task-orchestrator 不提供初始输入机制。
- **多实例隔离**：不同 `-data` 目录的多个进程完全独立，互不干扰；同一 data 目录同时只允许一个进程，后启动的会检测 PID 冲突并拒绝启动。
- **定时执行**：创建 pipeline 时可指定 cron 表达式（5 字段标准格式），由后台调度器每 30 秒检查一次触发条件。同一条 pipeline 每分钟最多触发一次，正在运行时跳过。重启后不追补离线期间遗漏的执行。
- **安全**：上传的 tar 包在解包时拦截路径穿越攻击（`../` 和绝对路径），仅提取安全路径。
- **优雅关闭**：SIGINT/SIGTERM 触发优雅关闭，先停止所有运行中的流水线，再退出 HTTP 服务（超时 30 秒）。

## 目录与存储结构

```
{data}/
├── tasks/                          ← 用户上传的 task 包（解包后只读，平台不写入）
│   ├── task-foo/
│   │   ├── README.md
│   │   ├── run.sh
│   │   └── ...                     ← 用户的内容
│   └── task-bar/
│       └── ...
│
├── task_meta/                      ← 平台维护的 task 元数据
│   ├── task-foo.json
│   └── task-bar.json
│
├── pipelines/                      ← 流水线定义（关联关系的唯一权威来源）
│   ├── pipeline-1.json
│   └── pipeline-2.json
│
├── runs/                           ← 运行数据
│   ├── run-001/                    ← pipeline-1 的某次运行
│   │   ├── events.log              ← 状态变更事件日志
│   │   ├── task-data-1/            ← 共享数据目录，双缓冲其一
│   │   ├── task-data-2/            ← 共享数据目录，双缓冲其二
│   │   ├── task-echo-load-0/      ← 同一 task 可多次出现，索引后缀区分
│   │   │   ├── stdout.log
│   │   │   ├── stderr.log
│   │   │   └── meta.json
│   │   ├── task-echo-load-1/
│   │   └── ai-security-auditer-2/
│   └── run-002/
│
├── orchestrator.log                ← 平台日志
└── orchestrator_state.json         ← 全局运行锁及多流水线运行状态 { pid, running_pipelines[] }
```

## 数据文件格式

### `task_meta/{task-name}.json`

```json
{
  "name": "task-foo",
  "package_path": "tasks/task-foo",
  "uploaded_at": "2026-05-15T10:00:00Z",
  "run_command": "./run.sh",
  "stop_command": "./stop.sh",
  "readme_path": "README.md",
  "timeout_enabled": true,
  "timeout_seconds": 30,
  "on_timeout": "fail"
}
```

- `timeout_enabled`: 是否启用超时。`false` 时 task 无超时限制。
- `timeout_seconds`: 超时秒数（仅 `timeout_enabled=true` 时生效）。
- `on_timeout`: 超时行为。`"fail"` — 超时后标记失败，阻止流水线继续；`"skip"` — 超时后标记 timeout，跳过当前 task 继续执行流水线。
- `continue_on_failure`: `true` 时 task 失败（exit 非零或超时但非 skip）后流水线继续执行下一个 task；`false`（默认）则停止。

注：不再存储 `pipelines` 字段。查询 task 关联了哪些 pipeline 时，遍历 `pipelines/` 目录动态计算。

### `pipelines/{pipeline-id}.json`

```json
{
  "id": "pipeline-1",
  "name": "我的流水线",
  "tasks": [
    {"name": "task-A", "timeout_seconds": 30, "on_timeout": "skip"},
    {"name": "task-A"},                    ← 同一 task 可重复出现
    {"name": "task-B", "continue_on_failure": true},
    {"name": "task-B"}
  ],
  "created_at": "2026-05-15T10:00:00Z",
  "status": "idle",
  "schedule": "0 9 * * *",
  "webhook_url": "https://hooks.example.com/notify"
}
```

- `tasks`: 数组，每项为对象，`name` 为 task 名称。
- `timeout_seconds`: 可选，覆盖 task 默认超时。`0`=禁用超时，`>0`=覆盖秒数，缺失=继承 task 默认。
- `on_timeout`: 可选，覆盖 task 默认超时行为。`"fail"` / `"skip"`，缺失=继承 task 默认。
- `continue_on_failure`: 可选，覆盖 task 默认失败继续行为。`true` / `false`，缺失=继承 task 默认。

Pipeline 自身状态：`idle` | `running`

`schedule` 可选，标准 5 字段 cron 表达式（分 时 日 月 周），空或缺失表示不启用定时执行。创建时可设定，空闲时可修改，运行中不可修改。

`webhook_url` 可选。流水线完成（成功/失败）时 POST JSON 通知到此 URL，手动停止不通知。创建时可设定，空闲时可修改，运行中不可修改。

### `runs/{run-id}/{task-name}-{index}/meta.json`

```json
{
  "task_name": "task-A",
  "run_id": "run-001",
  "pipeline_id": "pipeline-1",
  "status": "success",
  "started_at": "2026-05-15T10:01:00Z",
  "ended_at": "2026-05-15T10:02:30Z",
  "exit_code": 0,
  "index": 1
}
```

Run 中 task 实例状态：`pending` | `running` | `success` | `failed` | `stopped` | `crashed` | `timeout`

### `orchestrator_state.json`

```json
{
  "pid": 12345,
  "running_pipelines": [
    {"pipeline_id": "pipeline-1", "current_task": "task-A", "current_run_id": "run-001", "task_index": 0},
    {"pipeline_id": "pipeline-2", "current_task": "task-B", "current_run_id": "run-002", "task_index": 1}
  ]
}
```

`pid` 用于单实例锁和崩溃恢复。`running_pipelines` 数组记录当前在运行的所有流水线。

崩溃恢复：启动时检查 `pid` 对应进程是否存活。不存活则遍历 `running_pipelines`，将所有运行中的 task 标记为 `crashed`，逐条将 pipeline 状态复位为 `idle`，最后清空锁文件。

## 核心规则

### Task 管理

| 操作 | 规则 |
|---|---|
| 上传 | `task_xxx.tar` 文件名（去 `.tar`）即 task name，全局唯一，不可改名。解包时若顶层恰好一个目录则直接用，若为散文件则自动用 task name 创建目录包裹。同名拒绝上传。 |
| 自描述 | 包根目录下 `for-task-orchestrator.txt`，格式 `start: <cmd>` / `stop: <cmd>`，上传时自动解析，优先级高于默认的 `./run.sh` / `./stop.sh`。 |
| 解析 | 在包目录下大小写不敏感查找 `README.md` / `readme.md` / `readme` / `readme.txt`，优先级：`README.md` > `readme.md` > `readme` > `readme.txt`。找到则解析展示。 |
| 配置 | 运行/停止命令、超时设置（`timeout_enabled`、`timeout_seconds`、`on_timeout`）及失败继续（`continue_on_failure`）存入 `task_meta/{name}.json`，跨 pipeline 共享。 |
| 下载 | 将 task 包目录打包为 tar，通过 API 导出。 |
| 删除 | 查询所有 pipeline 文件，若有关联则拒绝。二次确认后删除包目录 `tasks/{name}/` + 元数据 `task_meta/{name}.json`。 |

### Pipeline 管理

| 操作 | 规则 |
|---|---|
| 创建 | 输入名称，不重名则创建。可附带 cron 表达式（5 字段标准格式）启用定时执行，可选 webhook URL。创建后可修改 schedule 和 webhook。 |
| 拖入 task | 关联 task 到 pipeline，自动继承 task 默认超时和失败继续设置。同一 task 可在同一 pipeline 中多次出现，每次出现是独立的 task 实例，由其在数组中的索引唯一标识。 |
| 配置 task | 单击 pipeline 中的 task 实例，弹出设置面板可配置超时秒数、超时行为（失败/跳过）和失败继续行为（继续/停止），仅影响当前实例，不影响其他同名实例。支持一键"重置为默认"清除所有覆盖。 |
| 移除 task | 单击 task 实例上的 × 按钮移除该实例。同一 task 的其他同名实例不受影响。 |
| 调整顺序 | 拖拽改变 task 实例在 pipeline 中的排序，配置属性随实例保留。 |
| 运行 | 从第一个 task 实例开始串行执行。多条流水线可同时运行互不干扰，每条产生独立的 `run_id`。Pipeline 状态变为 `running`，跑完或停止后回到 `idle`。 |
| 停止 | 仅 pipeline 状态为 `running` 时可用。执行 `stop_command` → 超时 10s 后 SIGKILL → 标记当前 task 实例 `stopped` → pipeline 状态回 `idle` → 链上后续不触发。 |
| 删除 | `running` 状态时不可删。二次确认后删除 pipeline 定义 + 该 pipeline 下所有 run 数据。 |

### 运行数据管理

| 操作 | 规则 |
|---|---|
| 日志存储 | stdout/stderr 存文件，事后查看，不实时推送。 |
| 事件日志 | 每个 run 目录下 `events.log` 记录 pipeline/task 状态变更，可通过 API 和前端按钮查看。 |
| 日志删除 | 按 pipeline 粒度、按 run 粒度均可删除。 |
| 数据隔离 | 不同 pipeline → 不同 run 目录；同一 run 内不同 task → 不同子目录。 |
| 磁盘监控 | 记录每个 run 目录的大小，界面上展示。 |
| 状态展示 | 运行历史表格默认展示每行 run 的状态（运行中/成功/失败），彩色标示。当前正在运行的 run 高亮显示（蓝色背景 + 左侧脉冲光条），每 3 秒自动刷新。 |

## 超时机制

Task 可设置超时和失败继续行为，均支持双层配置（task 默认 + pipeline 级覆盖）。

### Task 默认超时

在左侧面板编辑 task 时，可设置：

- **启用超时**（checkbox）：是否开启超时检测。
- **超时秒数**：超过此时间未完成则触发超时处理。
- **超时行为**：
  - `fail`（默认）：超时后 task 状态标记为 `timeout`，流水线停止。
  - `skip`：超时后 task 状态标记为 `timeout`，流水线跳过当前 task 继续执行下一个。

Task 默认超时对该 task 在所有 pipeline 中的出现均生效。

### Pipeline 级覆盖

在右侧 pipeline 视图中单击某个 task，弹出配置弹窗可单独覆盖超时和失败继续设置：

- **超时秒数**：设置具体秒数（0=禁用超时），预填当前实际生效值。
- **超时行为**：选择 `fail`（失败，阻止流水线）或 `skip`（跳过，继续执行），预填当前实际生效值。
- **失败继续**：选择"继续"或"停止"，预填当前实际生效值。
- **重置为默认**：一键清除所有 pipeline 级覆盖，恢复继承 task 默认值。

### 超时执行过程

1. 超时倒计时到达 → 发送 `SIGTERM` 给 task 进程。
2. 等待 10 秒 → 若进程未退出则发 `SIGKILL` 强杀。
3. 写 task 实例状态为 `timeout`（区别于主动停止 `stopped` 和异常退出 `failed`）。
4. 根据超时行为决定流水线是否继续。

## 失败继续（Continue-on-Failure）

默认情况下，task 执行失败（exit 非零或超时）后流水线停止。开启 continue-on-failure 后，失败仅记录状态，流水线继续执行下一个 task。

也支持双层配置（task 默认 + pipeline 级覆盖），与超时配置完全相同的继承机制。

### Task 默认

在左侧面板编辑 task 时，勾选"失败时继续流水线"即可。

### Pipeline 级覆盖

在右侧 pipeline 视图中单击某个 task，弹出配置弹窗可同时设置超时秒数、超时行为和失败继续行为，预填当前实际生效值。修改后点确定保存，重置为默认则一键清除所有覆盖。

### 与超时 skip 的关系

- 超时 `skip` 优先于 continue-on-failure 生效：超时且行为为 skip 时，无论 continue-on-failure 如何设置都继续。
- continue-on-failure 生效于：task exit 非零、超时且行为为 fail 等场景。

## 双缓冲数据传递

Task 之间通过 `task-data-1/` 和 `task-data-2/` 两个目录传递数据。每个 task 通过环境变量 `TASK_DATA_READ`、`TASK_DATA_WRITE` 获知读写路径。

```
Pipeline 启动前:  创建 task-data-1/ task-data-2/

task-1 (index=0, 偶数) 开始前:   清空 task-data-1/
task-1 运行中:                  写入 task-data-1/（给 task-2）
task-1 结束后:                  清空 task-data-2/

task-2 (index=1, 奇数) 开始前:   task-data-1 保留（上游数据）、task-data-2 已清空
task-2 运行中:                  读取 task-data-1/，写入 task-data-2/（给 task-3）
task-2 结束后:                  清空 task-data-1/

task-3 (index=2, 偶数) 开始前:   task-data-2 保留、task-data-1 已清空
task-3 运行中:                  读取 task-data-2/，写入 task-data-1/（给 task-4）
...

规律:
  - 偶数索引 task (0,2,4...): 清空 task-data-1，读取 task-data-2，写入 task-data-1
  - 奇数索引 task (1,3,5...): 清空 task-data-2，读取 task-data-1，写入 task-data-2

平台职责: 严格按上述时序做好目录的清空（rm -rf + mkdir），不干预 task 对 task-data 的读/写内容。
注: 流水线最后一个 task 写入 task-data 的输出无下游消费，属于正常行为。
```

## API 概览

| 方法 | 路径 | 说明 |
|---|---|---|
| GET/POST | `/api/tasks` | 列表 / 上传（multipart form，field: `file`） |
| GET/PUT/DELETE | `/api/tasks/{name}` | 详情+README / 更新命令+超时+失败继续配置（timeout_enabled, timeout_seconds, on_timeout, continue_on_failure）/ 删除 |
| GET | `/api/tasks/{name}/download` | 下载 task 为 tar 包 |
| GET/POST | `/api/pipelines` | 列表 / 创建（可选 `webhook_url`） |
| GET/PUT/DELETE | `/api/pipelines/{id}` | 详情（含 task 实例及超时覆盖）/ 操作（add_task, remove_task, reorder, set_task_config, set_schedule, set_webhook）。remove_task 使用 `task_index`；reorder 使用 `task_indices`；set_task_config 使用 `task_index`；set_webhook 使用 `webhook_url`。 |
| POST | `/api/pipelines/{id}/start` | 启动流水线 |
| POST | `/api/pipelines/{id}/stop` | 停止当前流水线 |
| GET | `/api/runs` | 运行列表（支持 `?pipeline_id=` 过滤） |
| GET/DELETE | `/api/runs/{id}` | 运行详情+日志 / 删除单次运行 |
| GET | `/api/runs/{id}/events` | 运行事件日志（JSON 文本） |
| GET | `/api/state` | 全局运行状态 |

## Webhook 通知

为 pipeline 配置 `webhook_url` 后，流水线完成（成功或失败）时会向该 URL 发送 HTTP POST 请求，手动停止的流水线不触发通知。

### Payload 格式

```json
{
  "event": "pipeline_completed",
  "pipeline_id": "pipeline-2",
  "run_id": "run-pipeline-2-005",
  "status": "success",
  "task_count": 3,
  "started_at": "2026-05-20T02:00:00Z",
  "ended_at": "2026-05-20T02:05:30Z",
  "failed_task": ""
}
```

失败时 `status` 为 `"failed"`，`failed_task` 为首个失败的 task 名称。

### 可靠性

- Webhook POST 超时 10 秒，fire-and-forget，不影响流水线自身流程。
- 网络错误或非 2xx 响应仅记录日志，不重试。

## 项目结构

```
main.go              — 入口
go.mod               — Go 1.22+
internal/
  api/handler.go     — HTTP 路由与处理器
  task/task.go       — Task 元数据、上传/下载/自描述解析
  pipeline/pipeline.go — Pipeline CRUD 与 task 编排
  runner/runner.go   — 流水线执行、双缓冲、崩溃恢复
  runner/cron.go     — cron 表达式解析匹配
  logger/logger.go   — slog 日志初始化、文件轮转
web/
  templates/index.html — 主页面
  static/app.css       — 样式
  static/app.js        — 前端逻辑（SortableJS 拖拽编排）
  static/sortable.min.js — SortableJS 1.15.6 (vendored)
```

## 日志

- **结构化日志**：`log/slog` Text 格式，同时输出 stderr 和 `data/orchestrator.log`，支持 `--log-level` 控制级别。
- **日志轮转**：启动时 + 每 24h 定时轮转：超过 7 天的未压缩日志 gzip 压缩，超过 365 天的 `.gz` 删除。
- **日志查看器**：界面上查看任务 stdout/stderr 和 run 事件日志，支持手动刷新和 60 秒自动刷新（默认关闭）。

## 技术栈

| 层 | 选型 |
|---|---|
| 后端 | Go 纯标准库（`net/http`、`os/exec`、`archive/tar`、`encoding/json`、`html/template`、`embed`） |
| 前端 | 服务端渲染（`html/template`）+ vanilla JS |
| 拖拽 | SortableJS 1.15.6，vendor 单个 `.js` 文件进仓库，embed 进二进制 |
| 部署 | 交叉编译单二进制，拷贝即运行，零运行时依赖 |

## 后续扩展

- 流水线：暂停、恢复、循环执行
- Task：暂停、恢复、循环执行、超时重试
- 自动清理过期运行数据

## License

MIT License — 详见 [LICENSE](LICENSE)。Copyright (c) 2026 WangShuWei (JohnVX)
