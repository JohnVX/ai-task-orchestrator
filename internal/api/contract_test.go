package api

import (
	"encoding/json"
	"testing"
	"time"
)

// API contract tests: verify response structure for every endpoint the
// frontend depends on. These guard against accidental field removal or
// type changes that would break app.js at runtime.
//
// Each test documents the exact frontend code path that depends on the
// fields being verified.

// --- helpers ---

func requireField(t *testing.T, m map[string]interface{}, field, endpoint string) {
	t.Helper()
	if _, ok := m[field]; !ok {
		t.Errorf("[%s] required field %q missing", endpoint, field)
	}
}

func requireStringField(t *testing.T, m map[string]interface{}, field, endpoint string) {
	t.Helper()
	v, ok := m[field]
	if !ok {
		t.Errorf("[%s] required field %q missing", endpoint, field)
		return
	}
	if _, isStr := v.(string); !isStr {
		t.Errorf("[%s] field %q: expected string, got %T", endpoint, field, v)
	}
}

func requireBoolField(t *testing.T, m map[string]interface{}, field, endpoint string) {
	t.Helper()
	v, ok := m[field]
	if !ok {
		t.Errorf("[%s] required field %q missing", endpoint, field)
		return
	}
	if _, isBool := v.(bool); !isBool {
		t.Errorf("[%s] field %q: expected bool, got %T", endpoint, field, v)
	}
}

func requireNumField(t *testing.T, m map[string]interface{}, field, endpoint string) {
	t.Helper()
	v, ok := m[field]
	if !ok {
		t.Errorf("[%s] required field %q missing", endpoint, field)
		return
	}
	if _, isNum := v.(float64); !isNum {
		t.Errorf("[%s] field %q: expected number, got %T", endpoint, field, v)
	}
}

// --- GET /api/tasks ---
// JS: renderTaskList() → t.name, t.type; window.taskMetas[t.name] = t

func TestContractTaskList(t *testing.T) {
	h := newTestHandler(t)
	createTestTask(t, h, "ct-echo", "#!/bin/sh\necho ok\n")

	resp := doRequest(t, h, "GET", "/api/tasks", nil)
	mustStatus(t, resp, 200)
	tasks := decodeJSON[[]map[string]interface{}](t, resp)
	if len(tasks) == 0 {
		t.Fatal("expected at least 1 task")
	}
	for _, task := range tasks {
		requireStringField(t, task, "name", "GET /api/tasks")
		requireStringField(t, task, "package_path", "GET /api/tasks")
		requireStringField(t, task, "uploaded_at", "GET /api/tasks")
	}
}

// --- GET /api/tasks/{name} ---
// JS: showTaskDetail() → data.meta (name, type, run_command, stop_command,
//   timeout_enabled, timeout_seconds, on_timeout, continue_on_failure, retry_count)
//   data.readme

func TestContractTaskDetail(t *testing.T) {
	h := newTestHandler(t)
	createTestTask(t, h, "ct-detail", "#!/bin/sh\necho ok\n")
	createTestLLMTask(t, h, "ct-llm", "echo hello")

	resp := doRequest(t, h, "GET", "/api/tasks/ct-detail", nil)
	mustStatus(t, resp, 200)
	data := decodeMap(t, resp)
	meta, ok := data["meta"].(map[string]interface{})
	if !ok {
		t.Fatal("meta field missing or wrong type")
	}
	requireStringField(t, meta, "name", "GET /api/tasks/{name}.meta")
	requireStringField(t, meta, "run_command", "GET /api/tasks/{name}.meta")
	requireStringField(t, meta, "stop_command", "GET /api/tasks/{name}.meta")
	requireField(t, data, "readme", "GET /api/tasks/{name}")
	requireBoolField(t, meta, "timeout_enabled", "GET /api/tasks/{name}.meta")
	requireNumField(t, meta, "timeout_seconds", "GET /api/tasks/{name}.meta")
	requireBoolField(t, meta, "continue_on_failure", "GET /api/tasks/{name}.meta")
	requireNumField(t, meta, "retry_count", "GET /api/tasks/{name}.meta")
	// on_timeout may be omitted when empty (omitempty)

	// LLM task: type field must be present
	resp2 := doRequest(t, h, "GET", "/api/tasks/ct-llm", nil)
	mustStatus(t, resp2, 200)
	data2 := decodeMap(t, resp2)
	meta2 := data2["meta"].(map[string]interface{})
	requireStringField(t, meta2, "type", "GET /api/tasks/{name}.meta (llm-prompt)")
}

// --- GET /api/pipelines ---
// JS: renderPipelineList() → p.id, p.name, p.status

func TestContractPipelineList(t *testing.T) {
	h := newTestHandler(t)
	createTestPipeline(t, h, "ct-pipe-list")

	resp := doRequest(t, h, "GET", "/api/pipelines", nil)
	mustStatus(t, resp, 200)
	pipes := decodeJSON[[]map[string]interface{}](t, resp)
	if len(pipes) == 0 {
		t.Fatal("expected at least 1 pipeline")
	}
	for _, p := range pipes {
		requireStringField(t, p, "id", "GET /api/pipelines")
		requireStringField(t, p, "name", "GET /api/pipelines")
		requireStringField(t, p, "status", "GET /api/pipelines")
		requireStringField(t, p, "created_at", "GET /api/pipelines")
	}
}

// --- GET /api/pipelines/{id} ---
// JS: refreshCanvas() → data.pipeline (id, name, status, schedule, tasks[])
//   data.tasks[] (name, run_command, stop_command, readme, stage)
// Note: webhook_url, loop_count, schedule are omitempty — only present when set.
//   JS handles undefined gracefully: if (pipeline.schedule) ...

func TestContractPipelineDetail(t *testing.T) {
	h := newTestHandler(t)
	createTestTask(t, h, "ct-pd-a", "#!/bin/sh\necho a\n")
	createTestTask(t, h, "ct-pd-b", "#!/bin/sh\necho b\n")
	p := createTestPipeline(t, h, "ct-pipe-detail")
	mustAddTask(t, h, p.ID, "ct-pd-a")
	mustAddTask(t, h, p.ID, "ct-pd-b")
	doRequest(t, h, "PUT", "/api/pipelines/"+p.ID, map[string]interface{}{
		"action": "set_task_stage", "task_index": 0, "stage": "build",
	})
	doRequest(t, h, "PUT", "/api/pipelines/"+p.ID, map[string]interface{}{
		"action": "set_schedule", "schedule": "*/5 * * * *",
	})

	resp := doRequest(t, h, "GET", "/api/pipelines/"+p.ID, nil)
	mustStatus(t, resp, 200)
	data := decodeMap(t, resp)

	// Pipeline object
	pipe, ok := data["pipeline"].(map[string]interface{})
	if !ok {
		t.Fatal("pipeline field missing or wrong type")
	}
	requireStringField(t, pipe, "id", "GET /api/pipelines/{id}.pipeline")
	requireStringField(t, pipe, "name", "GET /api/pipelines/{id}.pipeline")
	requireStringField(t, pipe, "status", "GET /api/pipelines/{id}.pipeline")
	requireStringField(t, pipe, "created_at", "GET /api/pipelines/{id}.pipeline")
	// schedule is present because we set it
	requireStringField(t, pipe, "schedule", "GET /api/pipelines/{id}.pipeline")
	// pipeline.tasks[] — raw TaskRef list
	if _, ok := pipe["tasks"]; !ok {
		t.Fatal("pipeline.tasks field missing")
	}

	// Enriched tasks array (top-level "tasks")
	enrichedRaw, ok := data["tasks"]
	if !ok {
		t.Fatal("tasks field missing from response")
	}
	enrichedTasks := enrichedRaw.([]interface{})
	if len(enrichedTasks) < 2 {
		t.Fatalf("expected at least 2 enriched tasks, got %d", len(enrichedTasks))
	}
	for _, item := range enrichedTasks {
		task := item.(map[string]interface{})
		requireStringField(t, task, "name", "GET /api/pipelines/{id}.tasks[]")
		requireStringField(t, task, "run_command", "GET /api/pipelines/{id}.tasks[]")
		requireStringField(t, task, "stop_command", "GET /api/pipelines/{id}.tasks[]")
		requireField(t, task, "readme", "GET /api/pipelines/{id}.tasks[]")
	}
	// First task has stage set, verify it
	task0 := enrichedTasks[0].(map[string]interface{})
	if s, _ := task0["stage"].(string); s != "build" {
		t.Errorf("expected task[0].stage='build', got %q", s)
	}
}

// --- GET /api/state ---
// JS: refreshCanvas() → state.running_pipelines[].pipeline_id, .current_task,
//   .task_index, .current_run_id, .iteration, .loop_total

func TestContractState(t *testing.T) {
	h := newTestHandler(t)

	resp := doRequest(t, h, "GET", "/api/state", nil)
	mustStatus(t, resp, 200)
	state := decodeMap(t, resp)

	requireNumField(t, state, "pid", "GET /api/state")
	requireNumField(t, state, "start_time", "GET /api/state")
	requireField(t, state, "running_pipelines", "GET /api/state")

	rpRaw := state["running_pipelines"]
	if rpRaw == nil {
		return // null is valid
	}
	rps := rpRaw.([]interface{})
	for _, item := range rps {
		rp := item.(map[string]interface{})
		requireStringField(t, rp, "pipeline_id", "GET /api/state.running_pipelines[]")
		requireStringField(t, rp, "current_task", "GET /api/state.running_pipelines[]")
		requireStringField(t, rp, "current_run_id", "GET /api/state.running_pipelines[]")
		requireNumField(t, rp, "task_index", "GET /api/state.running_pipelines[]")
		requireNumField(t, rp, "iteration", "GET /api/state.running_pipelines[]")
		requireNumField(t, rp, "loop_total", "GET /api/state.running_pipelines[]")
	}
}

// --- GET /api/runs?pipeline_id=X ---
// JS: renderRunHistory() → r.run_id, r.pipeline_id, r.status, r.size, r.task_count

func TestContractRunList(t *testing.T) {
	h := newTestHandler(t)
	createTestTask(t, h, "ct-rl", "#!/bin/sh\necho ok\n")
	p := createTestPipeline(t, h, "ct-run-list")
	mustAddTask(t, h, p.ID, "ct-rl")
	startAndWait(t, h, p.ID, 10_000)

	resp := doRequest(t, h, "GET", "/api/runs?pipeline_id="+p.ID, nil)
	mustStatus(t, resp, 200)
	runs := decodeJSON[[]map[string]interface{}](t, resp)
	if len(runs) == 0 {
		t.Fatal("expected at least 1 run")
	}
	for _, run := range runs {
		requireStringField(t, run, "run_id", "GET /api/runs?pipeline_id")
		requireStringField(t, run, "pipeline_id", "GET /api/runs?pipeline_id")
		requireStringField(t, run, "status", "GET /api/runs?pipeline_id")
		requireNumField(t, run, "task_count", "GET /api/runs?pipeline_id")
		requireNumField(t, run, "size", "GET /api/runs?pipeline_id")
	}
}

// --- GET /api/runs{id} ---
// JS: refreshCanvas() & showRunDetail() → inst.task_name, inst.run_id,
//   inst.pipeline_id, inst.status, inst.started_at, inst.ended_at,
//   inst.exit_code, inst.index

func TestContractRunDetail(t *testing.T) {
	h := newTestHandler(t)
	createTestTask(t, h, "ct-rd", "#!/bin/sh\necho ok\n")
	p := createTestPipeline(t, h, "ct-run-detail")
	mustAddTask(t, h, p.ID, "ct-rd")
	runID, instances := startAndWait(t, h, p.ID, 10_000)

	if runID == "" {
		t.Fatal("expected non-empty run_id")
	}
	if len(instances) == 0 {
		t.Fatal("expected at least 1 task instance")
	}

	resp := doRequest(t, h, "GET", "/api/runs/"+runID, nil)
	mustStatus(t, resp, 200)
	insts := decodeJSON[[]map[string]interface{}](t, resp)
	if len(insts) == 0 {
		t.Fatal("expected at least 1 instance")
	}
	for _, inst := range insts {
		requireStringField(t, inst, "task_name", "GET /api/runs/{id}[]")
		requireStringField(t, inst, "run_id", "GET /api/runs/{id}[]")
		requireStringField(t, inst, "pipeline_id", "GET /api/runs/{id}[]")
		requireStringField(t, inst, "status", "GET /api/runs/{id}[]")
		requireStringField(t, inst, "started_at", "GET /api/runs/{id}[]")
		requireNumField(t, inst, "index", "GET /api/runs/{id}[]")
		// ended_at may be nil for running tasks (omitempty)
		// exit_code may be nil for tasks without exit (omitempty)
	}
}

// --- GET /api/runs ---
// JS: same as filtered run list

func TestContractAllRuns(t *testing.T) {
	h := newTestHandler(t)
	createTestTask(t, h, "ct-ar", "#!/bin/sh\necho ok\n")
	p := createTestPipeline(t, h, "ct-all-runs")
	mustAddTask(t, h, p.ID, "ct-ar")
	startAndWait(t, h, p.ID, 10_000)

	resp := doRequest(t, h, "GET", "/api/runs", nil)
	mustStatus(t, resp, 200)
	runs := decodeJSON[[]map[string]interface{}](t, resp)
	if len(runs) == 0 {
		t.Fatal("expected at least 1 run")
	}
	for _, run := range runs {
		requireStringField(t, run, "run_id", "GET /api/runs")
		requireStringField(t, run, "pipeline_id", "GET /api/runs")
		requireStringField(t, run, "status", "GET /api/runs")
	}
}

// --- POST /api/pipelines/{id}/start ---
// JS: startPipeline() → data.run_id

func TestContractStartPipeline(t *testing.T) {
	h := newTestHandler(t)
	createTestTask(t, h, "ct-start", "#!/bin/sh\necho ok\n")
	p := createTestPipeline(t, h, "ct-start-pipe")
	mustAddTask(t, h, p.ID, "ct-start")

	resp := doRequest(t, h, "POST", "/api/pipelines/"+p.ID+"/start", nil)
	mustStatus(t, resp, 200)
	data := decodeMap(t, resp)
	requireStringField(t, data, "run_id", "POST /api/pipelines/{id}/start")
	waitForPipelineDone(t, h, p.ID, 5*time.Second)
}

// --- POST /api/runs/{id}/continue ---
// JS: retryRun() → data.status (expects {"status":"ok"})

func TestContractContinueRun(t *testing.T) {
	h := newTestHandler(t)
	createTestTask(t, h, "ct-cont1", "#!/bin/sh\necho ok\n")
	createTestTask(t, h, "ct-cont2", "#!/bin/sh\nexit 1\n")
	p := createTestPipeline(t, h, "ct-cont-pipe")
	mustAddTask(t, h, p.ID, "ct-cont1")
	mustAddTask(t, h, p.ID, "ct-cont2")

	runID, _ := startAndWait(t, h, p.ID, 10_000)

	resp := doRequest(t, h, "POST", "/api/runs/"+runID+"/continue", map[string]interface{}{
		"pipeline_id": p.ID,
	})
	mustStatus(t, resp, 200)
	data := decodeMap(t, resp)
	requireStringField(t, data, "status", "POST /api/runs/{id}/continue")
	waitForPipelineDone(t, h, p.ID, 5*time.Second)
}

// --- State contract during parallel stage execution ---
// JS: refreshCanvas() → runningTasks = rp.current_task.split(',')
//   runningStageIdx = rp.task_index
// current_task must contain comma-separated task names during parallel execution.

func TestContractRunningStateDuringParallelStage(t *testing.T) {
	h := newTestHandler(t)
	createTestTask(t, h, "cs-slow", "#!/bin/sh\nsleep 1\necho done\n")
	createTestTask(t, h, "cs-fast", "#!/bin/sh\necho done\n")
	p := createTestPipeline(t, h, "cs-par-state")
	mustAddTask(t, h, p.ID, "cs-slow")
	mustAddTask(t, h, p.ID, "cs-fast")
	doRequest(t, h, "PUT", "/api/pipelines/"+p.ID, map[string]interface{}{
		"action": "set_task_stage", "task_index": 0, "stage": "parallel",
	})
	doRequest(t, h, "PUT", "/api/pipelines/"+p.ID, map[string]interface{}{
		"action": "set_task_stage", "task_index": 1, "stage": "parallel",
	})

	resp := doRequest(t, h, "POST", "/api/pipelines/"+p.ID+"/start", nil)
	mustStatus(t, resp, 200)

	// Immediately check state — both tasks should appear in current_task
	stateResp := doRequest(t, h, "GET", "/api/state", nil)
	state := decodeMap(t, stateResp)
	rps := state["running_pipelines"].([]interface{})
	found := false
	for _, item := range rps {
		rp := item.(map[string]interface{})
		if rp["pipeline_id"] == p.ID {
			found = true
			currentTask, _ := rp["current_task"].(string)
			if currentTask == "" {
				t.Error("current_task should be non-empty during parallel stage")
			}
		}
	}
	if !found {
		t.Fatal("pipeline not found in running_pipelines")
	}
	waitForPipelineDone(t, h, p.ID, 5*time.Second)
}

// --- Enriched tasks have type for LLM tasks ---
// JS: renderPipelineTasks() → t.type === 'llm-prompt' ? 'LLM' : 'EXE'

func TestContractEnrichedTasksHaveType(t *testing.T) {
	h := newTestHandler(t)
	createTestTask(t, h, "ct-type-exe", "#!/bin/sh\necho ok\n")
	createTestLLMTask(t, h, "ct-type-llm", "echo hello")
	p := createTestPipeline(t, h, "ct-type-pipe")
	mustAddTask(t, h, p.ID, "ct-type-exe")
	mustAddTask(t, h, p.ID, "ct-type-llm")

	resp := doRequest(t, h, "GET", "/api/pipelines/"+p.ID, nil)
	mustStatus(t, resp, 200)
	data := decodeMap(t, resp)
	tasks := data["tasks"].([]interface{})

	task1 := tasks[1].(map[string]interface{})
	t1type, ok := task1["type"].(string)
	if !ok || t1type != "llm-prompt" {
		t.Errorf("expected llm-prompt task to have type='llm-prompt', got type=%q", t1type)
	}
}

// --- POST /api/pipelines ---
// JS: createPipeline() → data

func TestContractCreatePipeline(t *testing.T) {
	h := newTestHandler(t)

	resp := doRequest(t, h, "POST", "/api/pipelines", map[string]interface{}{
		"name": "ct-create-pipe",
	})
	mustStatus(t, resp, 201)
	data := decodeMap(t, resp)
	requireStringField(t, data, "id", "POST /api/pipelines")
	requireStringField(t, data, "name", "POST /api/pipelines")
	requireStringField(t, data, "status", "POST /api/pipelines")
	requireStringField(t, data, "created_at", "POST /api/pipelines")
}

// --- DELETE /api/pipelines/{id} ---
// JS: deletePipeline() → response

func TestContractDeletePipeline(t *testing.T) {
	h := newTestHandler(t)
	p := createTestPipeline(t, h, "ct-delete-pipe")

	resp := doRequest(t, h, "DELETE", "/api/pipelines/"+p.ID, nil)
	mustStatus(t, resp, 200)
	data := decodeMap(t, resp)
	requireStringField(t, data, "status", "DELETE /api/pipelines/{id}")
}

// --- DELETE /api/runs/{id} ---
// JS: deleteRun() → response

func TestContractDeleteRun(t *testing.T) {
	h := newTestHandler(t)
	createTestTask(t, h, "ct-del-run", "#!/bin/sh\necho ok\n")
	p := createTestPipeline(t, h, "ct-del-run-pipe")
	mustAddTask(t, h, p.ID, "ct-del-run")
	runID, _ := startAndWait(t, h, p.ID, 10_000)

	resp := doRequest(t, h, "DELETE", "/api/runs/"+runID, nil)
	mustStatus(t, resp, 200)
	data := decodeMap(t, resp)
	requireStringField(t, data, "status", "DELETE /api/runs/{id}")
}

// --- helper ---

func createTestLLMTask(t *testing.T, h *Handler, name, promptContent string) {
	t.Helper()
	files := map[string]string{
		name + "/prompt.md":                  promptContent,
		name + "/for-task-orchestrator.txt":  "type:llm-prompt\nrun_command:claude -p --print",
	}
	tarPath := makeTar(t, name, files)
	resp := uploadTaskViaMultipart(t, h, tarPath)
	if resp.StatusCode != 201 {
		body, _ := json.Marshal(decodeMap(t, resp))
		t.Fatalf("failed to create LLM task %s: status %d body %s", name, resp.StatusCode, body)
	}
}
