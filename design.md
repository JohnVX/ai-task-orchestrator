# ai-task-orchestrator — AI Agent 任务编排与运行管理系统 设计文档 v0.2

---

## 设计原则

- **与 AI/大模型解耦**：ai-task-orchestrator 是通用任务编排与进程管理平台，不感知 task 内部是否是 AI agent。task 可以是任意可执行程序（调用大模型 API 的脚本、数据处理程序、模型训练任务等）。
- **舞台与演员分离**：ai-task-orchestrator 提供"舞台和调度"，task 是"演员"。舞台不需要知道演员在演什么。

---

## 一、核心概念

| 概念 | 说明 |
|---|---|
| **Task** | 用户上传的 AI agent 执行单元。task name 即全局唯一标识，不可改名。ai-task-orchestrator 不关心其内部实现。 |
| **Pipeline** | 多个 task 的有序编排，严格串行执行。 |
| **Run** | 某条 pipeline 的一次实际执行，包含各 task 实例的运行数据。 |

---

## 二、全局约束

- **全局运行锁**：同一时刻最多只有一条流水线在运行。ai-task-orchestrator 崩溃后通过 PID 存活检测自动解锁。
- **严格串行**：流水线内 task 逐个执行，不支持并行。想并发可自行在 task 内实现。
- **停止 = 失败**：手动停止流水线 → stop_command 执行 → 超时 N 秒后 SIGKILL 强杀 → 标记当前 task failed → 链上后续不触发。
- **上游数据自理**：第一个 task 的输入由 task 自身解决，ai-task-orchestrator 不提供初始输入机制。

---

## 三、目录与存储结构

```
{data}/
├── tasks/                          ← 用户上传的 task 包（解包后只读，ai-task-orchestrator 不写入）
│   ├── task-foo/
│   │   ├── README.md
│   │   ├── run.sh
│   │   └── ...                     ← 用户的内容
│   └── task-bar/
│       └── ...
│
├── task_meta/                      ← ai-task-orchestrator 维护的 task 元数据
│   ├── task-foo.json
│   └── task-bar.json
│
├── pipelines/                      ← 流水线定义（关联关系的唯一权威来源）
│   ├── pipeline-1.json
│   └── pipeline-2.json
│
├── runs/                           ← 运行数据
│   ├── run-001/                    ← pipeline-1 的某次运行
│   │   ├── task-data-1/            ← 共享数据目录，双缓冲其一
│   │   ├── task-data-2/            ← 共享数据目录，双缓冲其二
│   │   ├── task-A/
│   │   │   ├── stdout.log
│   │   │   ├── stderr.log
│   │   │   └── meta.json
│   │   ├── task-B/
│   │   └── task-C/
│   └── run-002/
│
└── orchestrator_state.json                  ← 全局运行锁 { running_pipeline, current_task, current_run_id, pid }
```

---

## 四、数据文件格式

### `task_meta/{task-name}.json`

```json
{
  "name": "task-foo",
  "package_path": "tasks/task-foo",
  "uploaded_at": "2026-05-15T10:00:00Z",
  "run_command": "./run.sh",
  "stop_command": "./stop.sh",
  "readme_path": "README.md"
}
```

注：不再存储 `pipelines` 字段。查询 task 关联了哪些 pipeline 时，遍历 `pipelines/` 目录动态计算。

### `pipelines/{pipeline-id}.json`

```json
{
  "id": "pipeline-1",
  "name": "我的流水线",
  "tasks": ["task-A", "task-B", "task-C"],
  "created_at": "2026-05-15T10:00:00Z",
  "status": "idle"
}
```

Pipeline 自身状态：`idle` | `running`

### `runs/{run-id}/{task-name}/meta.json`

```json
{
  "task_name": "task-A",
  "run_id": "run-001",
  "pipeline_id": "pipeline-1",
  "status": "success",
  "started_at": "2026-05-15T10:01:00Z",
  "ended_at": "2026-05-15T10:02:30Z",
  "exit_code": 0
}
```

Run 中 task 实例状态：`pending` | `running` | `success` | `failed` | `stopped` | `crashed`

### `orchestrator_state.json`

```json
{
  "running_pipeline": null,
  "current_task": null,
  "current_run_id": null,
  "pid": 12345
}
```

崩溃恢复：ai-task-orchestrator 启动时检查 `pid` 对应进程是否存活。不存活则判定为脏数据，清理锁，将上次 run 中 `running` 状态的 task 实例标记为 `crashed`。

---

## 五、核心规则

### Task 管理

| 操作 | 规则 |
|---|---|
| 上传 | `task_xxx.tar` 文件名（去 `.tar`）即 task name，全局唯一，不可改名。解包时若顶层恰好一个目录则直接用，若为散文件则自动用 task name 创建目录包裹。同名拒绝上传。 |
| 解析 | 在包目录下大小写不敏感查找 `readme.md` / `README.md` / `readme` / `readme.txt`，优先级：`README.md` > `readme.md` > `readme` > `readme.txt`。找到则解析展示，找不到不展示。 |
| 配置 | 运行/停止命令存入 `task_meta/{name}.json`，跨 pipeline 共享。 |
| 删除 | 查询所有 pipeline 文件，若有关联则拒绝。二次确认后删除包目录 `tasks/{name}/` + 元数据 `task_meta/{name}.json`。 |

### Pipeline 管理

| 操作 | 规则 |
|---|---|
| 创建 | 输入名称，不重名则创建。 |
| 拖入 task | 关联 task 到 pipeline。同一 task 可在同一 pipeline 中出现多次（暂不禁止）。 |
| 拖出 task | 解除关联并删除该 pipeline 下该 task 的运行数据。UI 上提示"脱出将删除历史运行数据"。 |
| 调整顺序 | 拖拽改变 task 在 pipeline 中的排序。 |
| 运行 | 从第一个 task 开始串行执行。前提：全局运行锁空闲且 ai-task-orchestrator 状态健康。每次运行产生一个 run_id。Pipeline 状态变为 `running`，跑完或停止后回到 `idle`。 |
| 停止 | 仅 pipeline 状态为 `running` 时可用。执行 `stop_command` → 超时 10s 后 SIGKILL → 标记当前 task 实例 `stopped` → pipeline 状态回 `idle` → 链上后续不触发。 |
| 删除 | `running` 状态时不可删。二次确认后删除 pipeline 定义 + 该 pipeline 下所有 run 数据 + 该 pipeline 下所有 task 的运行数据。 |

### 运行数据管理

| 操作 | 规则 |
|---|---|
| 日志存储 | stdout/stderr 存文件，事后查看，不实时推送。 |
| 日志删除 | 按 pipeline 粒度删除。 |
| 数据隔离 | 不同 pipeline → 不同 run 目录；同一 run 内不同 task → 不同子目录。 |
| 磁盘监控 | 记录每个 run 目录的大小，界面上展示。 |

---

## 六、双缓冲数据传递机制

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
  - 偶数索引 task (0,2,4...): 清空 task-data-1, 读取 task-data-2, 写入 task-data-1
  - 奇数索引 task (1,3,5...): 清空 task-data-2, 读取 task-data-1, 写入 task-data-2

ai-task-orchestrator 职责: 严格按上述时序做好目录的清空（rm -rf + mkdir），不干预 task 对 task-data 的读/写内容。

注: 流水线最后一个 task 写入 task-data 的输出无下游消费，属于正常行为。
```

---

## 七、技术栈

| 层 | 选型 |
|---|---|
| 后端 | Go，纯标准库（`net/http`、`os/exec`、`archive/tar`、`encoding/json`、`html/template`、`embed`） |
| 前端 | 服务端渲染（Go templates）+ 少量 vanilla JS |
| 拖拽 | SortableJS，vendor 单个 `.js` 文件进仓库，embed 进二进制 |
| 部署 | 单二进制文件拷贝，零运行时依赖。交叉编译适配隔离环境。 |

---

## 八、后续扩展（v0.1 不做）

- 流水线：暂停、恢复、循环执行
- Task：暂停、恢复、循环执行、超时重试
- 同一 task 在同一 pipeline 中重复出现的问题（若实际遇到再处理）
```

