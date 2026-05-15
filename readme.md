# ai-task-orchestrator

轻量级任务编排与运行管理平台。将任意可执行程序打包为 task，拖拽编排为 pipeline，串行执行并自动管理数据传递。

**与 AI 解耦** — task 可以是任意可执行程序，平台不感知内部实现。**单二进制部署** — Go 编译，零运行时依赖。

## 快速开始

```bash
# 构建
go build -o orchestrator .

# 启动（默认 data 目录 ./data，端口 8080）
./orchestrator

# 自定义配置
./orchestrator -data /var/lib/orchestrator -port 9090
```

## 核心概念

| 概念 | 说明 |
|---|---|
| **Task** | 用户上传的 tar 包（含 `run.sh`、可选的 `stop.sh` 和 `README.md`）。Task name 取自文件名，全局唯一，不可改名 |
| **Pipeline** | 多个 task 的有序编排，严格串行执行，同一时刻最多一条流水线运行 |
| **Run** | Pipeline 的一次实际执行，包含每个 task 的运行状态、日志和数据 |

### 双缓冲数据传递

Task 之间通过 `task-data-1/` 和 `task-data-2/` 两个目录传递数据：

- 偶数索引 task：清空 task-data-1 → 读取 task-data-2 → 写入 task-data-1
- 奇数索引 task：清空 task-data-2 → 读取 task-data-1 → 写入 task-data-2

每个 task 通过环境变量 `TASK_DATA_READ`、`TASK_DATA_WRITE` 获知读写路径。

## API 概览

| 方法 | 路径 | 说明 |
|---|---|---|
| GET/POST | `/api/tasks` | 列表 / 上传（multipart form，field: `file`） |
| GET/PUT/DELETE | `/api/tasks/{name}` | 详情+README / 更新命令 / 删除 |
| GET/POST | `/api/pipelines` | 列表 / 创建 |
| GET/PUT/DELETE | `/api/pipelines/{id}` | 详情 / 增删排序 task / 删除 |
| POST | `/api/pipelines/{id}/start` | 启动流水线 |
| POST | `/api/pipelines/{id}/stop` | 停止当前流水线 |
| GET | `/api/runs` | 运行列表（支持 `?pipeline_id=` 过滤） |
| GET | `/api/runs/{id}` | 运行详情 / 日志（`?log=1&task=xxx`） |
| GET | `/api/state` | 全局运行状态 |

## 设计要点

- **全局运行锁** — 同时最多一个流水线运行，通过 `orchestrator_state.json` 实现
- **崩溃恢复** — 启动时 PID 存活检测，自动清理脏锁并标记 crashed
- **停止策略** — 执行 stop_command → 等 10s → SIGKILL 强杀 → 标记失败 → 后续 task 不触发
- **数据归属** — Pipeline 文件是 task 关联唯一权威来源；查询 task 关联了哪些 pipeline 时动态扫描计算
- **删除保护** — Task 被 pipeline 引用时拒绝删除；Pipeline 运行时拒绝删除
- **单文件只读** — 上传的 task 包内容只读，平台不写入用户目录
- **日志磁盘监控** — 记录每个 run 目录大小，界面展示

## 项目结构

```
main.go              — 入口
go.mod               — Go 1.22+
internal/
  api/handler.go     — HTTP 路由与处理器
  task/task.go       — Task 元数据与生命周期管理
  pipeline/pipeline.go — Pipeline CRUD 与 task 编排
  runner/runner.go   — 流水线执行、双缓冲、崩溃恢复
web/
  templates/index.html — 主页面
  static/app.css       — 样式
  static/app.js        — 前端逻辑（SortableJS 拖拽编排）
  static/sortable.min.js — SortableJS 1.15.6 (vendored)
```

## 技术栈

- **后端**: Go 纯标准库（net/http、os/exec、archive/tar、encoding/json、html/template、embed）
- **前端**: 服务端渲染（html/template）+ vanilla JS
- **拖拽**: SortableJS (vendored)
- **部署**: 交叉编译单二进制，拷贝即运行
