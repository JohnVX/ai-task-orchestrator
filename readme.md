# ai-task-orchestrator

轻量级任务编排与运行管理平台。将任意可执行程序打包为 task，拖拽编排为 pipeline，支持 stage 内并行、stage 间串行执行，自动管理数据传递。

**单二进制部署** — Go 编译，零运行时依赖。**内置 LLM Agent** — `llm-prompt` 类型 task 由 Claude Code 直接执行。

## 设计原则

- **舞台与演员分离**：ai-task-orchestrator 提供"舞台和调度"，task 是"演员"。支持 `self-contained`（自包含可执行脚本）和 `llm-prompt`（LLM 提示词任务）两种 task 类型。
- **Agent 可替换**：LLM Agent 通过接口抽象，`--llm-agent` 指定。找不到 agent 时仅警告，不影响 self-contained 任务正常运行。

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
| `-max-runs` | `100` | 每条 pipeline 最大保留 run 数量，超出删最早的 (0=不限制) |
| `-llm-agent` | `claude-code` | LLM 提示词任务使用的 agent，目前仅支持 `claude-code` |

## 核心概念

| 概念 | 说明 |
|---|---|
| **Task** | 用户上传的 tar 包。Task name 取自文件名，全局唯一，不可改名。支持两种类型：`self-contained`（默认，自包含可执行脚本）和 `llm-prompt`（LLM 提示词任务，包含 `prompt.md`，由 LLM Agent 执行）。类型通过 `for-task-orchestrator.txt` 的 `type:` 字段声明。 |
| **Stage** | Pipeline 中相邻同名 stage 的 task 形成一组，组内并发执行（goroutines + WaitGroup），组间串行。未设置 stage 的 task 为隐式独立 stage，行为与之前完全一致。 |
| **Pipeline** | 多个 task 的有序编排。支持同一 task 在同一 pipeline 中多次出现（如 taskA → taskA → taskB → taskB），每个出现称为一个 task 实例，由其在数组中的索引唯一标识。 |
| **Run** | Pipeline 的一次实际执行，包含每个 task 的运行状态、日志和数据。 |

### 全局约束

- **多流水线并发**：不同流水线可以同时运行，互不干扰，每条流水线有独立的数据目录和运行控制。
- **并行 Stage**：Stage 内 task 并发执行（goroutine + WaitGroup），stage 间串行。相邻同名 stage 的 task 自动归为一组。未设置 stage 的 task 为隐式独立 stage，行为与旧版完全一致。各 task 仍有独立的 stdout/stderr/超时/重试。Stage 内 task 共享双缓冲 write 目录，用户自行管理文件名避免冲突。
- **停止 = 失败**：手动停止流水线 → stop_command 执行 → 超时 10s 后 SIGKILL 强杀 → 标记当前 task failed → 链上后续不触发。
- **续跑（从失败点继续）**：流水线因 task 失败/timeout/手动停止中断后，可从断点处继续执行。重跑失败的 task，后续 task 依次执行。同一 run 目录复用，不覆盖已有日志。前端 task 标红指示中断位置。循环执行时续跑会保持当前迭代位置（如循环 3 次在第 2 次停止，续跑后从第 2 次继续而非重新开始）。
- **上游数据自理**：第一个 task 的输入由 task 自身解决，ai-task-orchestrator 不提供初始输入机制。
- **多实例隔离**：不同 `-data` 目录的多个进程完全独立，互不干扰；同一 data 目录同时只允许一个进程，后启动的会检测 PID 冲突并拒绝启动。通过比较进程启动时间（`/proc/[pid]/stat` 的 starttime）防止 PID 复用误判。
- **定时执行**：创建 pipeline 时可指定 cron 表达式（5 字段标准格式），由后台调度器每 30 秒检查一次触发条件。同一条 pipeline 每分钟最多触发一次，正在运行时跳过。重启后不追补离线期间遗漏的执行。
- **安全**：上传的 tar 包在解包时拦截路径穿越攻击（`../` 和绝对路径），仅提取安全路径。
- **HTTP 超时保护**：HTTP Server 配置了 ReadTimeout/WriteTimeout (60s)、IdleTimeout (120s)、ReadHeaderTimeout (10s)，防止慢客户端占用连接资源。
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
│   ├── run-000001/                 ← pipeline-1 的某次运行
│   │   ├── events.log              ← 状态变更事件日志
│   │   ├── task-data-1/            ← 共享数据目录，双缓冲其一
│   │   ├── task-data-2/            ← 共享数据目录，双缓冲其二
│   │   ├── task-echo-load-0/      ← 同一 task 可多次出现，索引后缀区分
│   │   │   ├── stdout.log
│   │   │   ├── stderr.log
│   │   │   └── meta.json
│   │   ├── task-echo-load-1/
│   │   └── ai-security-auditer-2/
│   └── run-000002/
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
  "type": "self-contained",
  "readme_path": "README.md",
  "timeout_enabled": true,
  "timeout_seconds": 30,
  "on_timeout": "fail",
  "continue_on_failure": false,
  "retry_count": 0
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
    {"name": "task-A", "stage": "build", "timeout_seconds": 30, "on_timeout": "skip", "retry_count": 2},
    {"name": "task-A"},                    ← 同一 task 可重复出现
    {"name": "task-B", "continue_on_failure": true},
    {"name": "task-B"}
  ],
  "created_at": "2026-05-15T10:00:00Z",
  "status": "idle",
  "schedule": "0 9 * * *",
  "webhook_url": "https://hooks.example.com/notify",
  "loop_count": 2
}
```

- `tasks`: 数组，每项为对象，`name` 为 task 名称。`stage` 可选项（omitempty），用于并行 stage 分组。
- `timeout_seconds`: 可选，覆盖 task 默认超时。`0`=禁用超时，`>0`=覆盖秒数，缺失=继承 task 默认。
- `on_timeout`: 可选，覆盖 task 默认超时行为。`"fail"` / `"skip"`，缺失=继承 task 默认。
- `continue_on_failure`: 可选，覆盖 task 默认失败继续行为。`true` / `false`，缺失=继承 task 默认。

Pipeline 自身状态：`idle` | `running`

`schedule` 可选，标准 5 字段 cron 表达式（分 时 日 月 周），空或缺失表示不启用定时执行。创建时可设定，空闲时可修改，运行中不可修改。

`webhook_url` 可选。流水线完成（成功/失败）时 POST JSON 通知到此 URL，手动停止不通知。创建时可设定，空闲时可修改，运行中不可修改。

`loop_count` 可选。配置后每次触发 pipeline 自动执行指定次数的完整 run。`null`/未配置=不循环（默认，一次触发一次执行），`0`=永久循环直到手动停止，`N>0`=执行 N 次。每次循环迭代产生独立的 run 目录和 events.log。单次迭代失败不影响后续迭代。手动停止 → 当前迭代走 stop → 取消剩余循环。空闲时可修改，运行中不可修改。

### `runs/{run-id}/{task-name}-{index}/meta.json`

```json
{
  "task_name": "task-A",
  "run_id": "run-000001",
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
  "start_time": 9876543,
  "running_pipelines": [
    {"pipeline_id": "pipeline-1", "current_task": "task-A", "current_run_id": "run-000001", "task_index": 0, "iteration": 1, "loop_total": 3}
  ]
}
```

`pid` 和 `start_time` 用于单实例锁和崩溃恢复。`start_time` 记录进程启动时间（`/proc/[pid]/stat` 的 starttime 字段），防止 PID 复用误判。`running_pipelines` 数组记录当前在运行的所有流水线。

崩溃恢复：启动时检查 `pid` + `start_time` 是否匹配存活进程。不存活（或 PID 被复用）则遍历 `running_pipelines`，将所有运行中的 task 标记为 `crashed`，逐条将 pipeline 状态复位为 `idle`，最后清空锁文件。

## 核心规则

### Task 管理

| 操作 | 规则 |
|---|---|
| 上传 | `task_xxx.tar` 文件名（去 `.tar`）即 task name，全局唯一，不可改名。解包时若顶层恰好一个目录则直接用，若为散文件则自动用 task name 创建目录包裹。同名拒绝上传。Task name 仅允许字母、数字、下划线、连字符和点。 |
| 自描述 | 包根目录下 `for-task-orchestrator.txt`，格式 `type: <type>`（可选）、`start: <cmd>` / `stop: <cmd>`。上传时自动解析，`start:`/`stop:` 优先级高于默认的 `./run.sh` / `./stop.sh`。 |
| 解析 | 在包目录下大小写不敏感查找 `README.md` / `readme.md` / `readme` / `readme.txt`，优先级：`README.md` > `readme.md` > `readme` > `readme.txt`。找到则解析展示。 |
| 配置 | 运行/停止命令、超时设置（`timeout_enabled`、`timeout_seconds`、`on_timeout`）、失败继续（`continue_on_failure`）及超时重试（`retry_count`）存入 `task_meta/{name}.json`，跨 pipeline 共享。 |
| 下载 | 将 task 包目录打包为 tar，通过 API 导出。 |
| 删除 | 查询所有 pipeline 文件，若有关联则拒绝。二次确认后删除包目录 `tasks/{name}/` + 元数据 `task_meta/{name}.json`。 |

#### Task 类型

`for-task-orchestrator.txt` 支持 `type:` 字段声明任务类型：

| `type` 值 | 说明 |
|---|---|
| `self-contained` | 默认类型。自包含任务，需要 `start:` 命令（或默认 `./run.sh`）。平台直接 `sh -c` 执行。 |
| `llm-prompt` | LLM 提示词任务。包内必须包含 `prompt.md` 文件。忽略 `start:`/`stop:` 字段，由 LLM Agent（`--llm-agent` 指定，默认 `claude-code`）在 task 包目录下执行。Agent 读取 prompt.md 内容并追加执行指令前缀后调用 Claude CLI。stdout.log 头部记录完整 prompt 输入，后接 Agent 输出。SIGTERM 信号停止。 |

`llm-prompt` 包示例：

```
my-llm-task.tar
├── for-task-orchestrator.txt    ← 内容: type: llm-prompt
├── prompt.md                    ← LLM 提示词（Agent 入口）
└── script.py                    ← prompt.md 中可以引用 ./script.py
```

### Pipeline 管理

| 操作 | 规则 |
|---|---|
| 创建 | 输入名称，不重名则创建。可附带 cron 表达式（5 字段标准格式）启用定时执行，可选 webhook URL 和循环执行次数（`loop_count`，0=永久）。创建后可修改 schedule、webhook 和 loop_count。 |
| 拖入 task | 关联 task 到 pipeline，自动继承 task 默认超时和失败继续设置。同一 task 可在同一 pipeline 中多次出现，每次出现是独立的 task 实例，由其在数组中的索引唯一标识。 |
| 配置 task | 单击 pipeline 中的 task 实例，弹出设置面板可配置超时秒数、超时行为（失败/跳过）和失败继续行为（继续/停止），仅影响当前实例，不影响其他同名实例。支持一键"重置为默认"清除所有覆盖。 |
| 移除 task | 单击 task 实例上的 × 按钮移除该实例。同一 task 的其他同名实例不受影响。 |
| 调整顺序 | 拖拽改变 task 实例在 pipeline 中的排序，配置属性随实例保留。 |
| 运行 | 从第一个 task 实例开始串行执行。多条流水线可同时运行互不干扰，每条产生独立的 `run_id`。Pipeline 状态变为 `running`，跑完或停止后回到 `idle`。 |
| 停止 | 仅 pipeline 状态为 `running` 时可用。执行 `stop_command` → 超时 10s 后 SIGKILL → 标记当前 task 实例 `stopped` → pipeline 状态回 `idle` → 链上后续不触发。 |
| 续跑 | 从已停止/失败的 run 的断点继续执行。自动定位最后一个非成功（failed/timeout/stopped/crashed）的 task 实例并从该处重跑，后续 task 依次执行。同一 run 目录复用，不覆盖已有日志。全部已成功的 run 不可续跑。 |
| 删除 | `running` 状态时不可删。二次确认后删除 pipeline 定义 + 该 pipeline 下所有 run 数据。 |

### 运行数据管理

| 操作 | 规则 |
|---|---|
| 日志存储 | stdout/stderr 存文件，事后查看，不实时推送。 |
| 事件日志 | 每个 run 目录下 `events.log` 记录 pipeline/task 状态变更，可通过 API 和前端按钮查看。 |
| 日志删除 | 按 pipeline 粒度、按 run 粒度均可删除。 |
| 数据隔离 | 不同 pipeline → 不同 run 目录；同一 run 内不同 task → 不同子目录。 |
| 磁盘监控 | 记录每个 run 目录的大小，界面上展示。 |
| 自动清理 | 每条 pipeline 最多保留 N 个 run（`-max-runs` 参数，默认 100），超出后自动删除最早的 run 目录。启动时和每 24 小时执行一次。设为 0 禁用。正在运行的 pipeline 跳过不清理。 |
| 状态展示 | 运行历史表格默认展示每行 run 的状态（运行中/成功/失败），彩色标示。当前正在运行的 run 高亮显示（蓝色背景 + 左侧脉冲光条），每 3 秒自动刷新。 |
| 任务时长 | Pipeline 右侧每个 task 显示执行时长：已完成/超时/停止/失败显示静态灰色时长，正在运行显示蓝色实时跳动计时（每 5 秒更新）。取自最新 run 的 task 实例时间戳。 |
| 失败高亮 | Pipeline 非运行状态时，最后一个非成功的 task 实例在前端以红色边框和高亮背景标示，指示续跑断点位置。 |
| 续跑按钮 | Pipeline 非运行状态且最新 run 非成功时，展示"续跑"按钮，点击即从断点继续执行。前端展示 run ID 便于识别。 |

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

### 超时重试（Retry）

超时后可自动重试。仅对超时生效，非零退出的失败不触发重试。手动停止不重试。

```
超时 → retriesRemaining > 0 ?
       ├─ 是 → 重新执行 task（立即，无延迟），retriesRemaining--
       └─ 否 → 走 on_timeout 逻辑（fail/skip）
```

每个 task 的单次执行（包括重试）都有独立的超时倒计时。

也支持双层配置（task 默认 + pipeline 级覆盖），与超时配置完全相同的继承机制。`retry_count=0` 表示不重试（默认）。

events.log 会记录每次重试：

```
task=heavy-job[1] event=timeout timeout=30s attempt=1
task=heavy-job[1] event=retry attempt=2/3
task=heavy-job[1] event=timeout timeout=30s attempt=2
task=heavy-job[1] event=retry attempt=3/3
task=heavy-job[1] status=success
```

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
  - 按 stage 序号交替（而非 task 序号）。偶数 stage (0,2,4...): 清空 task-data-1，读取 task-data-2，写入 task-data-1
  - 奇数 stage (1,3,5...): 清空 task-data-2，读取 task-data-1，写入 task-data-2

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
| GET/PUT/DELETE | `/api/pipelines/{id}` | 详情（含 task 实例及超时覆盖）/ 操作（add_task, remove_task, reorder, set_task_config, set_schedule, set_webhook, set_loop, set_task_stage, set_tasks）。remove_task 使用 `task_index`；reorder 使用 `task_indices`；set_task_config 使用 `task_index`；set_loop 使用 `loop_count`。 |
| POST | `/api/pipelines/{id}/start` | 启动流水线；支持已停止/失败的历史 run 续跑（需传入 `run_id`） |
| POST | `/api/pipelines/{id}/stop` | 停止当前流水线 |
| GET | `/api/runs` | 运行列表（支持 `?pipeline_id=` 过滤），按 run_id 降序（最新在前） |
| GET/DELETE | `/api/runs/{id}` | 运行详情+日志 / 删除单次运行 |
| GET | `/api/runs/{id}/events` | 运行事件日志（JSON 文本） |
| POST | `/api/runs/{id}/continue` | 续跑：从指定 run 的第一个非成功 task 实例继续执行，复用同一 run 目录 |
| GET | `/api/state` | 全局运行状态 |

## Webhook 通知

为 pipeline 配置 `webhook_url` 后，流水线完成（成功或失败）时会向该 URL 发送 HTTP POST 请求，手动停止的流水线不触发通知。

### Payload 格式

```json
{
  "event": "pipeline_completed",
  "pipeline_id": "pipeline-2",
  "run_id": "run-pipeline-2-000005",
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
  agent/agent.go     — LLM Agent 接口、注册表、Claude Code 实现
  api/handler.go     — HTTP 路由与处理器
  task/task.go       — Task 元数据（含 Type 字段）、上传/下载/自描述解析
  pipeline/pipeline.go — Pipeline CRUD 与 task 编排
  runner/runner.go   — 流水线执行（含 Agent 条件执行）、双缓冲、崩溃恢复
  runner/cron.go     — cron 表达式解析匹配
  logger/logger.go   — slog 日志初始化、文件轮转
  runner/cleanup.go  — 运行数据保留策略清理
  api/handler_test.go    — HTTP API 功能测试（106 tests）
  api/contract_test.go   — 前后端 API 契约测试（15 tests）
  runner/runner_test.go  — 执行器测试（24 tests）
  runner/cleanup_test.go — 清理策略测试（9 tests）
  pipeline/pipeline_test.go — Pipeline CRUD 测试（18 tests）
  task/task_test.go      — Task 描述符解析测试（4 tests）
web/
  templates/index.html — 主页面
  static/app.css       — 样式（含 task type badge）
  static/app.js        — 前端逻辑（SortableJS 拖拽编排、task 时长展示）
  static/sortable.min.js — SortableJS 1.15.6 (vendored)
```

## 测试

```bash
go test ./... -count=1   # 全部 176 个测试（~15s + ~110s 执行时间）
```

| 包 | 文件 | 测试数 | 覆盖内容 |
|---|------|--------|----------|
| `internal/api` | `handler_test.go` | 106 | HTTP API 功能测试，覆盖 task 生命周期（含 llm-prompt 上传验证）、pipeline 生命周期、流水线执行（成功/超时/跳过/失败继续/手动停止/续跑/并行 stage）、run 管理、状态管理、循环执行、数据清理、cron 验证、stage 编排（set_tasks/set_task_stage/remove_task） |
| `internal/api` | `contract_test.go` | 15 | 前后端 API 契约测试，验证 11 个 API 端点的响应结构（字段存在性与类型），防止后端改动导致前端 JS 运行时出错 |
| `internal/runner` | `runner_test.go` | 24 | 执行器单元测试，task 元数据读写、run 信息、日志、超时重试、循环执行、PID 复用检测 |
| `internal/runner` | `cleanup_test.go` | 9 | 运行数据清理：禁用/限制内/超出/多流水线/运行中跳过/混合状态/空目录 |
| `internal/pipeline` | `pipeline_test.go` | 18 | Pipeline CRUD：重复 task、独立配置、索引移除、重排保留配置、边界情况、持久化、跨流水线隔离 |
| `internal/task` | `task_test.go` | 4 | Task 描述符解析：llm-prompt 类型、self-contained 类型、无 type、缺失文件 |

测试风格：纯 Go 标准库（`testing` + `net/http/httptest`），零外部断言框架。API 测试通过真实 HTTP 路由执行，使用真实 `task/pipeline/runner` Manager 全连线。

## 日志

- **结构化日志**：`log/slog` Text 格式，同时输出 stderr 和 `data/orchestrator.log`，支持 `--log-level` 控制级别。HTTP 请求日志中间件记录每个 API 请求的 method/path/status/duration/remote。
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

（暂无）

## License

MIT License — 详见 [LICENSE](LICENSE)。Copyright (c) 2026 WangShuWei (JohnVX)
