# ai-task-orchestrator — Project Memory

## Architecture

- **Go backend** (single binary): `main.go` → `internal/` (agent, api, task, pipeline, runner, logger)
- **Frontend**: server-rendered `html/template` + vanilla JS (`app.js`) + SortableJS drag-drop
- **Data**: JSON files on disk (`task_meta/`, `pipelines/`, `runs/`, `orchestrator_state.json`)
- **No database, no ORM** — pure file-based persistence

## Key Domain Concepts

- **Task types**: `self-contained` (shell commands) vs `llm-prompt` (prompt.md + LLM Agent)
- **LLM Agent**: per-task configurable (`claude-code` or `opencode`), stored in `task.Meta.LLMAgent`
  - Set via: `for-task-orchestrator.txt` `agent:` line, sidebar dropdown, or API `llm_agent` field
  - Default: `claude-code` for llm-prompt tasks on upload
  - Runner: looks up `meta.LLMAgent` first, falls back to global `--llm-agent` flag
- **Pipeline**: ordered task chain, supports same task multiple times (distinguished by index)
- **Stage**: adjacent tasks with same stage name run concurrently (goroutines + WaitGroup)
- **Dual-buffer data**: `task-data-1/` and `task-data-2/` alternate per stage
- **Timeout/Continue/Retry**: dual-layer config (task default + pipeline override)

## UI Layout

- **Left sidebar** (`#task-detail` panel):
  - `self-contained` tasks: show Run/Stop Command inputs
  - `llm-prompt` tasks: show LLM Agent dropdown (claude-code/opencode), hide Run/Stop
  - Shared: timeout config, continue-on-failure, retry count
- **Right pipeline canvas** (`#task-config-modal`):
  - `llm-prompt` tasks: show LLM Agent as read-only text at top
  - All tasks: timeout/continue/retry/stage overrides (editable when pipeline idle)

## Build & Test

```bash
go build ./...                              # compile all
go test ./... -count=1                      # run all 228 tests (~130s)
go test ./internal/task/... -count=1        # task tests only (44)
go test ./internal/api/... -count=1         # API tests only (107+15=122)
```

## Key File Map

| File | Purpose |
|------|---------|
| `internal/task/task.go` | Meta struct (name, type, llm_agent, run/stop, timeout...), upload, SetConfig |
| `internal/task/task.go` parseTaskDescriptor | Reads `for-task-orchestrator.txt` (type, start, stop) |
| `internal/task/task.go` parseAgentFromDescriptor | Reads `agent:` line from same file |
| `internal/pipeline/pipeline.go` | TaskRef (name, overrides, stage, llm_agent), CRUD |
| `internal/runner/runner.go` | Execution: per-task agent lookup, stage concurrency, dual-buffer |
| `internal/agent/agent.go` | Agent interface + registry (claude-code, opencode) |
| `internal/api/handler.go` | HTTP routes, taskInfo enrichment (includes llm_agent) |
| `web/templates/index.html` | Sidebar (cmd-fields + agent-fields), modal (agent-section) |
| `web/static/app.js` | showTaskDetail (type-based field switching), configureTask (read-only agent) |

## Recent Changes (2026-05-29)

- **Per-task LLM agent config**: each llm-prompt task can independently select claude-code or opencode
  - Backend: `task.Meta.LLMAgent` field, `parseAgentFromDescriptor()`, runner per-task lookup
  - Frontend: sidebar agent dropdown for llm-prompt, pipeline modal read-only agent display
  - Tests: +4 tests (parseAgentFromDescriptor x3, SetConfigLLMAgent x1), total 228