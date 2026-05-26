package api

import (
	"archive/tar"
	"bytes"
	"encoding/json"
	"html/template"
	"io"
	"log/slog"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ai-task-orchestrator/internal/agent"
	"github.com/ai-task-orchestrator/internal/pipeline"
	"github.com/ai-task-orchestrator/internal/runner"
	"github.com/ai-task-orchestrator/internal/task"
)

// --- helpers ---

func newTestHandler(t *testing.T) *Handler {
	t.Helper()
	dir := t.TempDir()
	dataDir := filepath.Join(dir, "data")
	for _, d := range []string{"tasks", "task_meta", "pipelines", "runs"} {
		if err := os.MkdirAll(filepath.Join(dataDir, d), 0755); err != nil {
			t.Fatal(err)
		}
	}
	taskMgr := task.NewManager(filepath.Join(dataDir, "tasks"), filepath.Join(dataDir, "task_meta"), filepath.Join(dataDir, "pipelines"))
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	runMgr := runner.NewManager(filepath.Join(dataDir, "runs"), dataDir, taskMgr, logger, agent.MustGet("claude-code"))
	pipeMgr := pipeline.NewManager(filepath.Join(dataDir, "pipelines"), taskMgr, runMgr)
	runMgr.SetPipelineStatusSetter(pipeMgr)
	tmpl := template.Must(template.New("index").Parse(""))
	return NewHandler(taskMgr, pipeMgr, runMgr, dataDir, tmpl, http.Dir(dir), logger)
}

func makeTar(t *testing.T, name string, files map[string]string) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "tar-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	path := filepath.Join(dir, name+".tar")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	tw := tar.NewWriter(f)
	for filename, content := range files {
		hdr := &tar.Header{
			Name: filename,
			Mode: 0755,
			Size: int64(len(content)),
		}
		if err := tw.WriteHeader(hdr); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write([]byte(content)); err != nil {
			t.Fatal(err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	return f.Name()
}

func doRequest(t *testing.T, h *Handler, method, path string, body interface{}) *http.Response {
	t.Helper()
	var r io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		r = bytes.NewReader(b)
	}
	req := httptest.NewRequest(method, path, r)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	w := httptest.NewRecorder()
	h.Router().ServeHTTP(w, req)
	return w.Result()
}

func mustStatus(t *testing.T, resp *http.Response, want int) {
	t.Helper()
	if resp.StatusCode != want {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected status %d, got %d: %s", want, resp.StatusCode, string(body))
	}
}

func decodeJSON[T any](t *testing.T, resp *http.Response) T {
	t.Helper()
	var v T
	if err := json.NewDecoder(resp.Body).Decode(&v); err != nil {
		t.Fatalf("decode JSON: %v", err)
	}
	return v
}

func decodeMap(t *testing.T, resp *http.Response) map[string]interface{} {
	t.Helper()
	var v map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&v); err != nil {
		t.Fatalf("decode JSON: %v", err)
	}
	return v
}

func uploadTaskViaMultipart(t *testing.T, h *Handler, tarPath string) *http.Response {
	t.Helper()
	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)
	part, err := w.CreateFormFile("file", filepath.Base(tarPath))
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(tarPath)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := io.Copy(part, f); err != nil {
		t.Fatal(err)
	}
	w.Close()
	req := httptest.NewRequest("POST", "/api/tasks", &buf)
	req.Header.Set("Content-Type", w.FormDataContentType())
	rr := httptest.NewRecorder()
	h.Router().ServeHTTP(rr, req)
	return rr.Result()
}

func createTestTask(t *testing.T, h *Handler, name, script string) task.Meta {
	t.Helper()
	path := makeTar(t, name, map[string]string{"run.sh": script})
	resp := uploadTaskViaMultipart(t, h, path)
	mustStatus(t, resp, 201)
	return decodeJSON[task.Meta](t, resp)
}

func createTestPipeline(t *testing.T, h *Handler, name string) pipeline.Pipeline {
	t.Helper()
	resp := doRequest(t, h, "POST", "/api/pipelines", map[string]interface{}{"name": name})
	mustStatus(t, resp, 201)
	return decodeJSON[pipeline.Pipeline](t, resp)
}

func mustAddTask(t *testing.T, h *Handler, pipelineID, taskName string) {
	t.Helper()
	resp := doRequest(t, h, "PUT", "/api/pipelines/"+pipelineID, map[string]interface{}{
		"action":    "add_task",
		"task_name": taskName,
	})
	mustStatus(t, resp, 200)
}

func startAndWait(t *testing.T, h *Handler, pipelineID string, timeout time.Duration) (runID string, instances []runner.TaskInstance) {
	t.Helper()
	resp := doRequest(t, h, "POST", "/api/pipelines/"+pipelineID+"/start", nil)
	mustStatus(t, resp, 200)
	runID = decodeMap(t, resp)["run_id"].(string)

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		stateResp := doRequest(t, h, "GET", "/api/state", nil)
		if stateResp.StatusCode != 200 {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		s := decodeJSON[runner.OrchestratorState](t, stateResp)
		stillRunning := false
		for _, rp := range s.RunningPipelines {
			if rp.PipelineID == pipelineID {
				stillRunning = true
				break
			}
		}
		if !stillRunning {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	infoResp := doRequest(t, h, "GET", "/api/runs/"+runID, nil)
	mustStatus(t, infoResp, 200)
	instances = decodeJSON[[]runner.TaskInstance](t, infoResp)
	return runID, instances
}

func waitForPipelineDone(t *testing.T, h *Handler, pipelineID string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp := doRequest(t, h, "GET", "/api/state", nil)
		if resp.StatusCode != 200 {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		s := decodeJSON[runner.OrchestratorState](t, resp)
		done := true
		for _, rp := range s.RunningPipelines {
			if rp.PipelineID == pipelineID {
				done = false
				break
			}
		}
		if done {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("pipeline %s did not finish within %v", pipelineID, timeout)
}

func ptr[T ~int | ~string | ~bool](v T) *T { return &v }

// ===== Task Lifecycle Tests =====

func TestUploadTaskValid(t *testing.T) {
	h := newTestHandler(t)
	m := createTestTask(t, h, "my-task", "#!/bin/sh\necho ok\nexit 0\n")
	if m.Name != "my-task" {
		t.Fatalf("expected name my-task, got %s", m.Name)
	}
	if m.RunCommand != "./run.sh" {
		t.Fatalf("expected run_command ./run.sh, got %s", m.RunCommand)
	}
	if m.PackagePath != filepath.Join("tasks", "my-task") {
		t.Fatalf("expected package_path tasks/my-task, got %s", m.PackagePath)
	}
}

func TestUploadTaskInvalidName(t *testing.T) {
	h := newTestHandler(t)
	path := makeTar(t, "@invalid!", map[string]string{"run.sh": "#!/bin/sh\necho ok\n"})
	// Rename to match the invalid name
	badPath := filepath.Join(filepath.Dir(path), "@invalid!.tar")
	os.Rename(path, badPath)
	resp := uploadTaskViaMultipart(t, h, badPath)
	mustStatus(t, resp, 400)
	m := decodeMap(t, resp)
	if !strings.Contains(m["error"].(string), "invalid") {
		t.Fatalf("expected invalid name error, got: %v", m["error"])
	}
}

func TestUploadTaskDuplicate(t *testing.T) {
	h := newTestHandler(t)
	createTestTask(t, h, "dup-task", "#!/bin/sh\necho ok\n")
	// Upload the same task again
	path := makeTar(t, "dup-task", map[string]string{"run.sh": "#!/bin/sh\necho again\n"})
	resp := uploadTaskViaMultipart(t, h, path)
	mustStatus(t, resp, 400)
	m := decodeMap(t, resp)
	if !strings.Contains(m["error"].(string), "already exists") {
		t.Fatalf("expected duplicate error, got: %v", m["error"])
	}
}

func TestUploadTaskNotTar(t *testing.T) {
	h := newTestHandler(t)
	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)
	part, _ := w.CreateFormFile("file", "not-a-tar.txt")
	part.Write([]byte("hello"))
	w.Close()
	req := httptest.NewRequest("POST", "/api/tasks", &buf)
	req.Header.Set("Content-Type", w.FormDataContentType())
	rr := httptest.NewRecorder()
	h.Router().ServeHTTP(rr, req)
	mustStatus(t, rr.Result(), 400)
}

func TestUploadLLMPromptTask(t *testing.T) {
	h := newTestHandler(t)
	path := makeTar(t, "llm-test", map[string]string{
		"prompt.md":                 "Analyze this code.",
		"for-task-orchestrator.txt": "type: llm-prompt",
	})
	resp := uploadTaskViaMultipart(t, h, path)
	mustStatus(t, resp, 201)
	m := decodeJSON[task.Meta](t, resp)
	if m.Type != task.TypeLLMPrompt {
		t.Fatalf("expected type %q, got %q", task.TypeLLMPrompt, m.Type)
	}
	if m.RunCommand != "" {
		t.Fatalf("expected empty run_command for llm-prompt, got %q", m.RunCommand)
	}
	if m.StopCommand != "" {
		t.Fatalf("expected empty stop_command for llm-prompt, got %q", m.StopCommand)
	}
}

func TestUploadLLMPromptMissingPromptMD(t *testing.T) {
	h := newTestHandler(t)
	path := makeTar(t, "bad-llm", map[string]string{
		"run.sh":                    "#!/bin/sh\necho hi\n",
		"for-task-orchestrator.txt": "type: llm-prompt",
	})
	resp := uploadTaskViaMultipart(t, h, path)
	mustStatus(t, resp, 400)
	m := decodeMap(t, resp)
	if !strings.Contains(m["error"].(string), "prompt.md") {
		t.Fatalf("expected error about missing prompt.md, got: %v", m["error"])
	}
}

func TestUploadLLMPromptUnknownType(t *testing.T) {
	h := newTestHandler(t)
	path := makeTar(t, "unknown-type", map[string]string{
		"run.sh":                    "#!/bin/sh\necho hi\n",
		"for-task-orchestrator.txt": "type: unknown-type",
	})
	resp := uploadTaskViaMultipart(t, h, path)
	mustStatus(t, resp, 400)
	m := decodeMap(t, resp)
	if !strings.Contains(m["error"].(string), "unknown task type") {
		t.Fatalf("expected error about unknown type, got: %v", m["error"])
	}
}

func TestListTasks(t *testing.T) {
	h := newTestHandler(t)
	createTestTask(t, h, "task-a", "#!/bin/sh\necho a\n")
	createTestTask(t, h, "task-b", "#!/bin/sh\necho b\n")
	resp := doRequest(t, h, "GET", "/api/tasks", nil)
	mustStatus(t, resp, 200)
	tasks := decodeJSON[[]task.Meta](t, resp)
	if len(tasks) != 2 {
		t.Fatalf("expected 2 tasks, got %d", len(tasks))
	}
}

func TestListTasksEmpty(t *testing.T) {
	h := newTestHandler(t)
	resp := doRequest(t, h, "GET", "/api/tasks", nil)
	mustStatus(t, resp, 200)
	// Verify it returns empty array, not null
	var tasks []task.Meta
	json.NewDecoder(resp.Body).Decode(&tasks)
	if tasks == nil {
		t.Fatal("expected empty array, got null")
	}
}

func TestGetTaskWithReadme(t *testing.T) {
	h := newTestHandler(t)
	path := makeTar(t, "readme-task", map[string]string{
		"run.sh":    "#!/bin/sh\necho hi\n",
		"README.md": "# My Task\nThis is a test task.\n",
	})
	resp := uploadTaskViaMultipart(t, h, path)
	mustStatus(t, resp, 201)

	resp = doRequest(t, h, "GET", "/api/tasks/readme-task", nil)
	mustStatus(t, resp, 200)
	m := decodeMap(t, resp)
	if m["meta"] == nil {
		t.Fatal("expected meta field")
	}
	if m["readme"] == nil || m["readme"].(string) == "" {
		t.Fatal("expected readme content")
	}
}

func TestGetTaskNonExistent(t *testing.T) {
	h := newTestHandler(t)
	resp := doRequest(t, h, "GET", "/api/tasks/no-such-task", nil)
	mustStatus(t, resp, 404)
}

func TestUpdateTaskConfig(t *testing.T) {
	h := newTestHandler(t)
	createTestTask(t, h, "config-task", "#!/bin/sh\necho ok\n")

	resp := doRequest(t, h, "PUT", "/api/tasks/config-task", map[string]interface{}{
		"run_command":         "./custom.sh",
		"timeout_enabled":     true,
		"timeout_seconds":     30,
		"on_timeout":          "skip",
		"continue_on_failure": true,
		"retry_count":         3,
	})
	mustStatus(t, resp, 200)

	// Verify via GET
	resp = doRequest(t, h, "GET", "/api/tasks/config-task", nil)
	mustStatus(t, resp, 200)
	m := decodeMap(t, resp)
	meta := m["meta"].(map[string]interface{})
	if meta["run_command"] != "./custom.sh" {
		t.Fatalf("expected ./custom.sh, got %v", meta["run_command"])
	}
	if meta["timeout_enabled"] != true {
		t.Fatal("expected timeout_enabled=true")
	}
	if meta["retry_count"].(float64) != 3 {
		t.Fatalf("expected retry_count=3, got %v", meta["retry_count"])
	}
}

func TestUpdateTaskNonExistent(t *testing.T) {
	h := newTestHandler(t)
	resp := doRequest(t, h, "PUT", "/api/tasks/no-such", map[string]interface{}{
		"run_command": "./x.sh",
	})
	mustStatus(t, resp, 404)
}

func TestDeleteTaskReferencedByPipeline(t *testing.T) {
	h := newTestHandler(t)
	createTestTask(t, h, "used-task", "#!/bin/sh\necho ok\n")
	p := createTestPipeline(t, h, "ref-pipe")
	mustAddTask(t, h, p.ID, "used-task")

	resp := doRequest(t, h, "DELETE", "/api/tasks/used-task", nil)
	mustStatus(t, resp, 409)
	m := decodeMap(t, resp)
	if !strings.Contains(m["error"].(string), "used by") {
		t.Fatalf("expected 'used by' error, got: %v", m["error"])
	}
}

func TestDeleteTaskNotReferenced(t *testing.T) {
	h := newTestHandler(t)
	createTestTask(t, h, "free-task", "#!/bin/sh\necho ok\n")
	resp := doRequest(t, h, "DELETE", "/api/tasks/free-task", nil)
	mustStatus(t, resp, 200)

	resp = doRequest(t, h, "GET", "/api/tasks/free-task", nil)
	mustStatus(t, resp, 404)
}

func TestDownloadTask(t *testing.T) {
	h := newTestHandler(t)
	createTestTask(t, h, "dl-task", "#!/bin/sh\necho downloaded\n")
	resp := doRequest(t, h, "GET", "/api/tasks/dl-task/download", nil)
	mustStatus(t, resp, 200)
	if ct := resp.Header.Get("Content-Type"); ct != "application/x-tar" {
		t.Fatalf("expected Content-Type application/x-tar, got %s", ct)
	}
	if cd := resp.Header.Get("Content-Disposition"); !strings.Contains(cd, "dl-task.tar") {
		t.Fatalf("expected Content-Disposition with dl-task.tar, got %s", cd)
	}
}

// ===== Pipeline Lifecycle Tests =====

func TestCreatePipeline(t *testing.T) {
	h := newTestHandler(t)
	p := createTestPipeline(t, h, "test-pipe")
	if p.ID == "" {
		t.Fatal("expected generated id")
	}
	if p.Name != "test-pipe" {
		t.Fatalf("expected name test-pipe, got %s", p.Name)
	}
	if p.Status != "idle" {
		t.Fatalf("expected status idle, got %s", p.Status)
	}
	if len(p.Tasks) != 0 {
		t.Fatalf("expected empty tasks, got %d", len(p.Tasks))
	}
}

func TestCreatePipelineDuplicateName(t *testing.T) {
	h := newTestHandler(t)
	createTestPipeline(t, h, "dupe-pipe")
	resp := doRequest(t, h, "POST", "/api/pipelines", map[string]interface{}{"name": "DUPE-PIPE"})
	mustStatus(t, resp, 409)
}

func TestCreatePipelineNoName(t *testing.T) {
	h := newTestHandler(t)
	resp := doRequest(t, h, "POST", "/api/pipelines", map[string]interface{}{})
	mustStatus(t, resp, 400)
}

func TestCreatePipelineInvalidCron(t *testing.T) {
	h := newTestHandler(t)
	resp := doRequest(t, h, "POST", "/api/pipelines", map[string]interface{}{
		"name":     "bad-cron",
		"schedule": "not-a-cron-expression",
	})
	mustStatus(t, resp, 400)
}

func TestCreatePipelineValidCron(t *testing.T) {
	h := newTestHandler(t)
	resp := doRequest(t, h, "POST", "/api/pipelines", map[string]interface{}{
		"name":     "good-cron",
		"schedule": "0 9 * * 1-5",
	})
	mustStatus(t, resp, 201)
	p := decodeJSON[pipeline.Pipeline](t, resp)
	if p.Schedule != "0 9 * * 1-5" {
		t.Fatalf("expected schedule, got %s", p.Schedule)
	}
}

func TestCreatePipelineNegativeLoopCount(t *testing.T) {
	h := newTestHandler(t)
	resp := doRequest(t, h, "POST", "/api/pipelines", map[string]interface{}{
		"name":       "bad-loop",
		"loop_count": -1,
	})
	mustStatus(t, resp, 409)
}

func TestCreatePipelineZeroLoopCount(t *testing.T) {
	h := newTestHandler(t)
	resp := doRequest(t, h, "POST", "/api/pipelines", map[string]interface{}{
		"name":       "forever-loop",
		"loop_count": 0,
	})
	mustStatus(t, resp, 201)
	p := decodeJSON[pipeline.Pipeline](t, resp)
	if p.LoopCount == nil || *p.LoopCount != 0 {
		t.Fatalf("expected loop_count=0, got %v", p.LoopCount)
	}
}

func TestListPipelines(t *testing.T) {
	h := newTestHandler(t)
	createTestPipeline(t, h, "p1")
	createTestPipeline(t, h, "p2")
	resp := doRequest(t, h, "GET", "/api/pipelines", nil)
	mustStatus(t, resp, 200)
	pipes := decodeJSON[[]pipeline.Pipeline](t, resp)
	if len(pipes) != 2 {
		t.Fatalf("expected 2 pipelines, got %d", len(pipes))
	}
}

func TestListPipelinesEmpty(t *testing.T) {
	h := newTestHandler(t)
	resp := doRequest(t, h, "GET", "/api/pipelines", nil)
	mustStatus(t, resp, 200)
	var pipes []pipeline.Pipeline
	json.NewDecoder(resp.Body).Decode(&pipes)
	if pipes == nil {
		t.Fatal("expected empty array, got null")
	}
}

func TestGetPipelineWithEnrichedTasks(t *testing.T) {
	h := newTestHandler(t)
	createTestTask(t, h, "enrich-task", "#!/bin/sh\necho enriched\n")
	pl := createTestPipeline(t, h, "enrich-pipe")
	mustAddTask(t, h, pl.ID, "enrich-task")

	resp := doRequest(t, h, "GET", "/api/pipelines/"+pl.ID, nil)
	mustStatus(t, resp, 200)
	m := decodeMap(t, resp)
	if m["pipeline"] == nil || m["tasks"] == nil {
		t.Fatal("expected pipeline and tasks fields")
	}
	tasks := m["tasks"].([]interface{})
	if len(tasks) != 1 {
		t.Fatalf("expected 1 task, got %d", len(tasks))
	}
	t0 := tasks[0].(map[string]interface{})
	if t0["name"] != "enrich-task" {
		t.Fatalf("expected enrich-task, got %v", t0["name"])
	}
}

func TestGetPipelineNonExistent(t *testing.T) {
	h := newTestHandler(t)
	resp := doRequest(t, h, "GET", "/api/pipelines/no-such", nil)
	mustStatus(t, resp, 404)
}

func TestAddTask(t *testing.T) {
	h := newTestHandler(t)
	createTestTask(t, h, "add-me", "#!/bin/sh\necho added\n")
	p := createTestPipeline(t, h, "add-pipe")
	mustAddTask(t, h, p.ID, "add-me")

	resp := doRequest(t, h, "GET", "/api/pipelines/"+p.ID, nil)
	mustStatus(t, resp, 200)
	m := decodeMap(t, resp)
	tasks := m["tasks"].([]interface{})
	if len(tasks) != 1 {
		t.Fatalf("expected 1 task, got %d", len(tasks))
	}
}

func TestAddNonExistentTask(t *testing.T) {
	h := newTestHandler(t)
	p := createTestPipeline(t, h, "missing-task-pipe")
	resp := doRequest(t, h, "PUT", "/api/pipelines/"+p.ID, map[string]interface{}{
		"action":    "add_task",
		"task_name": "does-not-exist",
	})
	mustStatus(t, resp, 400)
}

func TestAddTaskToRunningPipeline(t *testing.T) {
	h := newTestHandler(t)
	createTestTask(t, h, "sleepy", "#!/bin/sh\nsleep 10\necho done\n")
	p := createTestPipeline(t, h, "running-add")
	mustAddTask(t, h, p.ID, "sleepy")

	doRequest(t, h, "POST", "/api/pipelines/"+p.ID+"/start", nil)
	time.Sleep(100 * time.Millisecond)

	resp := doRequest(t, h, "PUT", "/api/pipelines/"+p.ID, map[string]interface{}{
		"action":    "add_task",
		"task_name": "sleepy",
	})
	mustStatus(t, resp, 400)
	_ = resp

	// Wait for pipeline to finish
	waitForPipelineDone(t, h, p.ID, 15*time.Second)
}

func TestRemoveTaskByIndex(t *testing.T) {
	h := newTestHandler(t)
	createTestTask(t, h, "rm-me", "#!/bin/sh\necho rm\n")
	p := createTestPipeline(t, h, "remove-pipe")
	mustAddTask(t, h, p.ID, "rm-me")
	mustAddTask(t, h, p.ID, "rm-me")
	mustAddTask(t, h, p.ID, "rm-me")

	resp := doRequest(t, h, "PUT", "/api/pipelines/"+p.ID, map[string]interface{}{
		"action":     "remove_task",
		"task_index": 1,
	})
	mustStatus(t, resp, 200)

	resp = doRequest(t, h, "GET", "/api/pipelines/"+p.ID, nil)
	mustStatus(t, resp, 200)
	m := decodeMap(t, resp)
	tasks := m["tasks"].([]interface{})
	if len(tasks) != 2 {
		t.Fatalf("expected 2 tasks after removal, got %d", len(tasks))
	}
}

func TestRemoveTaskOutOfBounds(t *testing.T) {
	h := newTestHandler(t)
	createTestTask(t, h, "bounds", "#!/bin/sh\necho bounds\n")
	p := createTestPipeline(t, h, "bounds-pipe")
	mustAddTask(t, h, p.ID, "bounds")

	for _, idx := range []int{-1, 5} {
		resp := doRequest(t, h, "PUT", "/api/pipelines/"+p.ID, map[string]interface{}{
			"action":     "remove_task",
			"task_index": idx,
		})
		mustStatus(t, resp, 400)
	}
}

func TestReorderTasks(t *testing.T) {
	h := newTestHandler(t)
	createTestTask(t, h, "task-a", "#!/bin/sh\necho a\n")
	createTestTask(t, h, "task-b", "#!/bin/sh\necho b\n")
	createTestTask(t, h, "task-c", "#!/bin/sh\necho c\n")
	p := createTestPipeline(t, h, "reorder-pipe")
	mustAddTask(t, h, p.ID, "task-a")
	mustAddTask(t, h, p.ID, "task-b")
	mustAddTask(t, h, p.ID, "task-c")

	// Reorder to: C, A, B
	resp := doRequest(t, h, "PUT", "/api/pipelines/"+p.ID, map[string]interface{}{
		"action":       "reorder",
		"task_indices": []int{2, 0, 1},
	})
	mustStatus(t, resp, 200)

	resp = doRequest(t, h, "GET", "/api/pipelines/"+p.ID, nil)
	mustStatus(t, resp, 200)
	m := decodeMap(t, resp)
	tasks := m["tasks"].([]interface{})
	if tasks[0].(map[string]interface{})["name"] != "task-c" {
		t.Fatalf("expected task-c at index 0, got %v", tasks[0].(map[string]interface{})["name"])
	}
	if tasks[1].(map[string]interface{})["name"] != "task-a" {
		t.Fatalf("expected task-a at index 1, got %v", tasks[1].(map[string]interface{})["name"])
	}
}

func TestReorderWithDuplicates(t *testing.T) {
	h := newTestHandler(t)
	createTestTask(t, h, "dup-a", "#!/bin/sh\necho a\n")
	p := createTestPipeline(t, h, "dup-reorder")
	mustAddTask(t, h, p.ID, "dup-a")
	mustAddTask(t, h, p.ID, "dup-a")

	resp := doRequest(t, h, "PUT", "/api/pipelines/"+p.ID, map[string]interface{}{
		"action":       "reorder",
		"task_indices": []int{0, 0},
	})
	mustStatus(t, resp, 400)
	m := decodeMap(t, resp)
	if !strings.Contains(m["error"].(string), "duplicate") {
		t.Fatalf("expected duplicate index error, got: %v", m["error"])
	}
}

func TestReorderWithMismatchedCount(t *testing.T) {
	h := newTestHandler(t)
	createTestTask(t, h, "mm", "#!/bin/sh\necho mm\n")
	p := createTestPipeline(t, h, "mismatch")
	mustAddTask(t, h, p.ID, "mm")
	mustAddTask(t, h, p.ID, "mm")

	resp := doRequest(t, h, "PUT", "/api/pipelines/"+p.ID, map[string]interface{}{
		"action":       "reorder",
		"task_indices": []int{0},
	})
	mustStatus(t, resp, 400)
}

func TestSetSchedule(t *testing.T) {
	h := newTestHandler(t)
	p := createTestPipeline(t, h, "sched-pipe")
	resp := doRequest(t, h, "PUT", "/api/pipelines/"+p.ID, map[string]interface{}{
		"action":   "set_schedule",
		"schedule": "*/15 * * * *",
	})
	mustStatus(t, resp, 200)

	resp = doRequest(t, h, "GET", "/api/pipelines/"+p.ID, nil)
	mustStatus(t, resp, 200)
	m := decodeMap(t, resp)
	pp := m["pipeline"].(map[string]interface{})
	if pp["schedule"] != "*/15 * * * *" {
		t.Fatalf("expected schedule */15 * * * *, got %v", pp["schedule"])
	}
}

func TestSetInvalidSchedule(t *testing.T) {
	h := newTestHandler(t)
	p := createTestPipeline(t, h, "bad-sched")
	resp := doRequest(t, h, "PUT", "/api/pipelines/"+p.ID, map[string]interface{}{
		"action":   "set_schedule",
		"schedule": "garbage",
	})
	mustStatus(t, resp, 400)
}

func TestSetWebhook(t *testing.T) {
	h := newTestHandler(t)
	p := createTestPipeline(t, h, "webhook-pipe")
	resp := doRequest(t, h, "PUT", "/api/pipelines/"+p.ID, map[string]interface{}{
		"action":      "set_webhook",
		"webhook_url": "http://hooks.example.com/notify",
	})
	mustStatus(t, resp, 200)

	resp = doRequest(t, h, "GET", "/api/pipelines/"+p.ID, nil)
	mustStatus(t, resp, 200)
	m := decodeMap(t, resp)
	pp := m["pipeline"].(map[string]interface{})
	if pp["webhook_url"] != "http://hooks.example.com/notify" {
		t.Fatalf("expected webhook_url, got %v", pp["webhook_url"])
	}
}

func TestSetLoopCount(t *testing.T) {
	h := newTestHandler(t)
	p := createTestPipeline(t, h, "loop-pipe")
	resp := doRequest(t, h, "PUT", "/api/pipelines/"+p.ID, map[string]interface{}{
		"action":     "set_loop",
		"loop_count": 5,
	})
	mustStatus(t, resp, 200)

	resp = doRequest(t, h, "GET", "/api/pipelines/"+p.ID, nil)
	mustStatus(t, resp, 200)
	m := decodeMap(t, resp)
	pp := m["pipeline"].(map[string]interface{})
	lc := pp["loop_count"].(float64)
	if int(lc) != 5 {
		t.Fatalf("expected loop_count=5, got %v", lc)
	}
}

func TestSetTaskConfig(t *testing.T) {
	h := newTestHandler(t)
	createTestTask(t, h, "cfg-task", "#!/bin/sh\necho cfg\n")
	p := createTestPipeline(t, h, "cfg-pipe")
	mustAddTask(t, h, p.ID, "cfg-task")

	resp := doRequest(t, h, "PUT", "/api/pipelines/"+p.ID, map[string]interface{}{
		"action":              "set_task_config",
		"task_index":          0,
		"timeout_seconds":     45,
		"on_timeout":          "skip",
		"continue_on_failure": true,
		"retry_count":         2,
	})
	mustStatus(t, resp, 200)

	resp = doRequest(t, h, "GET", "/api/pipelines/"+p.ID, nil)
	mustStatus(t, resp, 200)
	m := decodeMap(t, resp)
	tasks := m["tasks"].([]interface{})
	t0 := tasks[0].(map[string]interface{})
	if t0["timeout_seconds"].(float64) != 45 {
		t.Fatalf("expected timeout_seconds=45, got %v", t0["timeout_seconds"])
	}
	if t0["on_timeout"] != "skip" {
		t.Fatalf("expected on_timeout=skip, got %v", t0["on_timeout"])
	}
}

func TestSetInvalidOnTimeout(t *testing.T) {
	h := newTestHandler(t)
	createTestTask(t, h, "bad-action", "#!/bin/sh\necho bad\n")
	p := createTestPipeline(t, h, "bad-action-pipe")
	mustAddTask(t, h, p.ID, "bad-action")

	resp := doRequest(t, h, "PUT", "/api/pipelines/"+p.ID, map[string]interface{}{
		"action":     "set_task_config",
		"task_index": 0,
		"on_timeout": "invalid_value",
	})
	mustStatus(t, resp, 400)
}

func TestSetConfigOutOfBounds(t *testing.T) {
	h := newTestHandler(t)
	createTestTask(t, h, "oob", "#!/bin/sh\necho oob\n")
	p := createTestPipeline(t, h, "oob-pipe")
	mustAddTask(t, h, p.ID, "oob")

	resp := doRequest(t, h, "PUT", "/api/pipelines/"+p.ID, map[string]interface{}{
		"action":          "set_task_config",
		"task_index":      99,
		"timeout_seconds": 10,
	})
	mustStatus(t, resp, 400)
}

func TestDeletePipeline(t *testing.T) {
	h := newTestHandler(t)
	p := createTestPipeline(t, h, "del-pipe")
	resp := doRequest(t, h, "DELETE", "/api/pipelines/"+p.ID, nil)
	mustStatus(t, resp, 200)

	resp = doRequest(t, h, "GET", "/api/pipelines/"+p.ID, nil)
	mustStatus(t, resp, 404)
}

func TestDeleteRunningPipeline(t *testing.T) {
	h := newTestHandler(t)
	createTestTask(t, h, "sleepy2", "#!/bin/sh\nsleep 10\necho zzz\n")
	p := createTestPipeline(t, h, "running-del")
	mustAddTask(t, h, p.ID, "sleepy2")

	doRequest(t, h, "POST", "/api/pipelines/"+p.ID+"/start", nil)
	time.Sleep(100 * time.Millisecond)

	resp := doRequest(t, h, "DELETE", "/api/pipelines/"+p.ID, nil)
	mustStatus(t, resp, 409)

	waitForPipelineDone(t, h, p.ID, 15*time.Second)
}

func TestDeleteNonExistentPipeline(t *testing.T) {
	h := newTestHandler(t)
	resp := doRequest(t, h, "DELETE", "/api/pipelines/no-such-pipe", nil)
	mustStatus(t, resp, 404)
}

// ===== Pipeline Execution Tests =====

func TestStartPipeline(t *testing.T) {
	h := newTestHandler(t)
	createTestTask(t, h, "quick-task", "#!/bin/sh\necho quick\nexit 0\n")
	p := createTestPipeline(t, h, "start-pipe")
	mustAddTask(t, h, p.ID, "quick-task")

	resp := doRequest(t, h, "POST", "/api/pipelines/"+p.ID+"/start", nil)
	mustStatus(t, resp, 200)
	m := decodeMap(t, resp)
	runID := m["run_id"].(string)
	if !strings.HasPrefix(runID, "run-") {
		t.Fatalf("expected run_id starting with run-, got %s", runID)
	}
}

func TestStartEmptyPipeline(t *testing.T) {
	h := newTestHandler(t)
	p := createTestPipeline(t, h, "empty-start")
	resp := doRequest(t, h, "POST", "/api/pipelines/"+p.ID+"/start", nil)
	mustStatus(t, resp, 400)
}

func TestStartAlreadyRunningPipeline(t *testing.T) {
	h := newTestHandler(t)
	createTestTask(t, h, "slow-task", "#!/bin/sh\nsleep 10\necho slow\n")
	p := createTestPipeline(t, h, "double-start")
	mustAddTask(t, h, p.ID, "slow-task")

	doRequest(t, h, "POST", "/api/pipelines/"+p.ID+"/start", nil)
	time.Sleep(100 * time.Millisecond)

	resp := doRequest(t, h, "POST", "/api/pipelines/"+p.ID+"/start", nil)
	mustStatus(t, resp, 409)

	waitForPipelineDone(t, h, p.ID, 15*time.Second)
}

func TestStartNonExistentPipeline(t *testing.T) {
	h := newTestHandler(t)
	resp := doRequest(t, h, "POST", "/api/pipelines/no-such/start", nil)
	mustStatus(t, resp, 404)
}

func TestRunCompletesSuccessfully(t *testing.T) {
	h := newTestHandler(t)
	createTestTask(t, h, "success-task", "#!/bin/sh\necho success\nexit 0\n")
	p := createTestPipeline(t, h, "success-pipe")
	mustAddTask(t, h, p.ID, "success-task")

	_, instances := startAndWait(t, h, p.ID, 10*time.Second)
	if len(instances) == 0 {
		t.Fatal("expected at least one instance")
	}
	for _, inst := range instances {
		if inst.Status != runner.TaskStatusSuccess {
			t.Fatalf("expected all success, got %s for task %s", inst.Status, inst.TaskName)
		}
	}
}

func TestTaskTimeoutWithFail(t *testing.T) {
	h := newTestHandler(t)
	createTestTask(t, h, "sleeper", "#!/bin/sh\nsleep 30\necho never\n")
	// Set timeout via task update
	doRequest(t, h, "PUT", "/api/tasks/sleeper", map[string]interface{}{
		"run_command":     "./run.sh",
		"timeout_enabled": true,
		"timeout_seconds": 1,
		"on_timeout":      "fail",
	})
	p := createTestPipeline(t, h, "timeout-fail")
	mustAddTask(t, h, p.ID, "sleeper")

	_, instances := startAndWait(t, h, p.ID, 15*time.Second)
	if len(instances) != 1 {
		t.Fatalf("expected 1 instance, got %d", len(instances))
	}
	if instances[0].Status != runner.TaskStatusTimeout {
		t.Fatalf("expected timeout, got %s", instances[0].Status)
	}
}

func TestTaskTimeoutWithSkip(t *testing.T) {
	h := newTestHandler(t)
	createTestTask(t, h, "skip-sleeper", "#!/bin/sh\nsleep 30\necho never\n")
	doRequest(t, h, "PUT", "/api/tasks/skip-sleeper", map[string]interface{}{
		"run_command":     "./run.sh",
		"timeout_enabled": true,
		"timeout_seconds": 1,
		"on_timeout":      "skip",
	})
	createTestTask(t, h, "post-skip", "#!/bin/sh\necho after-skip\nexit 0\n")
	p := createTestPipeline(t, h, "timeout-skip")
	mustAddTask(t, h, p.ID, "skip-sleeper")
	mustAddTask(t, h, p.ID, "post-skip")

	_, instances := startAndWait(t, h, p.ID, 15*time.Second)
	if len(instances) != 2 {
		t.Fatalf("expected 2 instances, got %d", len(instances))
	}
	if instances[0].Status != runner.TaskStatusTimeout {
		t.Fatalf("expected first task timeout, got %s", instances[0].Status)
	}
	if instances[1].Status != runner.TaskStatusSuccess {
		t.Fatalf("expected second task success (pipeline continued), got %s", instances[1].Status)
	}
}

func TestContinueOnFailure(t *testing.T) {
	h := newTestHandler(t)
	createTestTask(t, h, "failer", "#!/bin/sh\nexit 1\n")
	createTestTask(t, h, "after-fail", "#!/bin/sh\necho survived\nexit 0\n")
	p := createTestPipeline(t, h, "continue-fail")
	mustAddTask(t, h, p.ID, "failer")
	mustAddTask(t, h, p.ID, "after-fail")

	resp := doRequest(t, h, "PUT", "/api/pipelines/"+p.ID, map[string]interface{}{
		"action":              "set_task_config",
		"task_index":          0,
		"continue_on_failure": true,
	})
	mustStatus(t, resp, 200)

	_, instances := startAndWait(t, h, p.ID, 10*time.Second)
	if instances[0].Status != runner.TaskStatusFailed {
		t.Fatalf("expected first task failed, got %s", instances[0].Status)
	}
	if instances[1].Status != runner.TaskStatusSuccess {
		t.Fatalf("expected second task success (pipeline continued), got %s", instances[1].Status)
	}
}

func TestManualStop(t *testing.T) {
	h := newTestHandler(t)
	createTestTask(t, h, "long-sleep", "#!/bin/sh\nsleep 30\necho finally\n")
	p := createTestPipeline(t, h, "stop-pipe")
	mustAddTask(t, h, p.ID, "long-sleep")

	resp := doRequest(t, h, "POST", "/api/pipelines/"+p.ID+"/start", nil)
	mustStatus(t, resp, 200)
	runID := decodeMap(t, resp)["run_id"].(string)

	time.Sleep(500 * time.Millisecond)

	resp = doRequest(t, h, "POST", "/api/pipelines/"+p.ID+"/stop", nil)
	mustStatus(t, resp, 200)

	// Wait for pipeline to finish stopping
	time.Sleep(3 * time.Second)

	resp = doRequest(t, h, "GET", "/api/runs/"+runID, nil)
	mustStatus(t, resp, 200)
	instances := decodeJSON[[]runner.TaskInstance](t, resp)
	if len(instances) == 0 {
		t.Fatal("expected at least one instance")
	}
	if instances[0].Status != runner.TaskStatusStopped {
		t.Fatalf("expected stopped, got %s", instances[0].Status)
	}
}

func TestStopNonRunningPipeline(t *testing.T) {
	h := newTestHandler(t)
	p := createTestPipeline(t, h, "idle-stop")
	resp := doRequest(t, h, "POST", "/api/pipelines/"+p.ID+"/stop", nil)
	mustStatus(t, resp, 400)
}

// ===== ContinueRun Tests =====

func TestContinueRunAfterFailure(t *testing.T) {
	h := newTestHandler(t)
	createTestTask(t, h, "fail-once", "#!/bin/sh\nexit 1\n")
	p := createTestPipeline(t, h, "retry-pipe")
	mustAddTask(t, h, p.ID, "fail-once")

	_, instances := startAndWait(t, h, p.ID, 10*time.Second)
	runID := instances[0].RunID
	if instances[0].Status != runner.TaskStatusFailed {
		t.Fatalf("expected failed, got %s", instances[0].Status)
	}

	// Now ContinueRun
	resp := doRequest(t, h, "POST", "/api/runs/"+runID+"/continue", map[string]interface{}{
		"pipeline_id": p.ID,
	})
	// This will re-run and fail again since the script always exits 1
	// But the ContinueRun should NOT be rejected
	if resp.StatusCode != 200 {
		m := decodeMap(t, resp)
		t.Fatalf("expected ContinueRun to start, got %d: %v", resp.StatusCode, m)
	}
}

func TestContinueRunAllSucceeded(t *testing.T) {
	h := newTestHandler(t)
	createTestTask(t, h, "ok-task", "#!/bin/sh\nexit 0\n")
	p := createTestPipeline(t, h, "all-ok")
	mustAddTask(t, h, p.ID, "ok-task")

	_, instances := startAndWait(t, h, p.ID, 10*time.Second)
	runID := instances[0].RunID

	resp := doRequest(t, h, "POST", "/api/runs/"+runID+"/continue", map[string]interface{}{
		"pipeline_id": p.ID,
	})
	mustStatus(t, resp, 409)
}

func TestContinueRunCrossPipeline(t *testing.T) {
	h := newTestHandler(t)
	createTestTask(t, h, "cross-fail", "#!/bin/sh\nexit 1\n")
	pA := createTestPipeline(t, h, "cross-a")
	mustAddTask(t, h, pA.ID, "cross-fail")
	pB := createTestPipeline(t, h, "cross-b")
	mustAddTask(t, h, pB.ID, "cross-fail")

	_, instances := startAndWait(t, h, pA.ID, 10*time.Second)
	runA := instances[0].RunID

	resp := doRequest(t, h, "POST", "/api/runs/"+runA+"/continue", map[string]interface{}{
		"pipeline_id": pB.ID,
	})
	mustStatus(t, resp, 409)
	m := decodeMap(t, resp)
	if !strings.Contains(m["error"].(string), "does not belong") {
		t.Fatalf("expected cross-pipeline rejection, got: %v", m["error"])
	}
}

// ===== Run Management Tests =====

func TestListRuns(t *testing.T) {
	h := newTestHandler(t)
	createTestTask(t, h, "list-task", "#!/bin/sh\necho listed\nexit 0\n")
	p := createTestPipeline(t, h, "list-pipe")
	mustAddTask(t, h, p.ID, "list-task")
	startAndWait(t, h, p.ID, 10*time.Second)

	resp := doRequest(t, h, "GET", "/api/runs", nil)
	mustStatus(t, resp, 200)
	runs := decodeJSON[[]map[string]interface{}](t, resp)
	if len(runs) == 0 {
		t.Fatal("expected at least one run")
	}
	r := runs[0]
	if r["run_id"] == nil || r["pipeline_id"] == nil || r["status"] == nil {
		t.Fatal("expected run_id, pipeline_id, and status fields")
	}
}

func TestListRunsFilterByPipelineID(t *testing.T) {
	h := newTestHandler(t)
	createTestTask(t, h, "filter-task", "#!/bin/sh\necho filtered\nexit 0\n")
	p1 := createTestPipeline(t, h, "filter-a")
	mustAddTask(t, h, p1.ID, "filter-task")
	p2 := createTestPipeline(t, h, "filter-b")
	mustAddTask(t, h, p2.ID, "filter-task")
	startAndWait(t, h, p1.ID, 10*time.Second)
	startAndWait(t, h, p2.ID, 10*time.Second)

	resp := doRequest(t, h, "GET", "/api/runs?pipeline_id="+p1.ID, nil)
	mustStatus(t, resp, 200)
	runs := decodeJSON[[]map[string]interface{}](t, resp)
	for _, r := range runs {
		if r["pipeline_id"] != p1.ID {
			t.Fatalf("expected only runs for %s, got %v", p1.ID, r["pipeline_id"])
		}
	}
}

func TestListRunsEmpty(t *testing.T) {
	h := newTestHandler(t)
	resp := doRequest(t, h, "GET", "/api/runs", nil)
	mustStatus(t, resp, 200)
	var runs []map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&runs)
	if runs == nil {
		t.Fatal("expected empty array, got null")
	}
}

func TestGetRunInfo(t *testing.T) {
	h := newTestHandler(t)
	createTestTask(t, h, "info-a", "#!/bin/sh\necho a\n")
	createTestTask(t, h, "info-b", "#!/bin/sh\necho b\n")
	p := createTestPipeline(t, h, "info-pipe")
	mustAddTask(t, h, p.ID, "info-a")
	mustAddTask(t, h, p.ID, "info-b")

	_, instances := startAndWait(t, h, p.ID, 10*time.Second)
	if len(instances) != 2 {
		t.Fatalf("expected 2 instances, got %d", len(instances))
	}
	if instances[0].Index != 0 || instances[1].Index != 1 {
		t.Fatalf("expected indices 0 and 1, got %d and %d", instances[0].Index, instances[1].Index)
	}
	if instances[0].TaskName != "info-a" || instances[1].TaskName != "info-b" {
		t.Fatalf("expected info-a and info-b, got %s and %s", instances[0].TaskName, instances[1].TaskName)
	}
}

func TestGetRunLogs(t *testing.T) {
	h := newTestHandler(t)
	createTestTask(t, h, "logger", "#!/bin/sh\necho hello world\necho error msg >&2\nexit 0\n")
	p := createTestPipeline(t, h, "log-pipe")
	mustAddTask(t, h, p.ID, "logger")

	runID, _ := startAndWait(t, h, p.ID, 10*time.Second)

	resp := doRequest(t, h, "GET", "/api/runs/"+runID+"?log=1&task=logger&task_idx=0", nil)
	mustStatus(t, resp, 200)
	m := decodeMap(t, resp)
	if !strings.Contains(m["stdout"].(string), "hello world") {
		t.Fatalf("expected 'hello world' in stdout, got: %s", m["stdout"])
	}
	if !strings.Contains(m["stderr"].(string), "error msg") {
		t.Fatalf("expected 'error msg' in stderr, got: %s", m["stderr"])
	}
}

func TestGetRunLogsMissingTaskParam(t *testing.T) {
	h := newTestHandler(t)
	resp := doRequest(t, h, "GET", "/api/runs/run-x-001?log=1", nil)
	mustStatus(t, resp, 400)
}

func TestGetRunEvents(t *testing.T) {
	h := newTestHandler(t)
	createTestTask(t, h, "event-task", "#!/bin/sh\nexit 0\n")
	p := createTestPipeline(t, h, "event-pipe")
	mustAddTask(t, h, p.ID, "event-task")

	runID, _ := startAndWait(t, h, p.ID, 10*time.Second)

	resp := doRequest(t, h, "GET", "/api/runs/"+runID+"/events", nil)
	mustStatus(t, resp, 200)
	m := decodeMap(t, resp)
	if m["events"] == nil || !strings.Contains(m["events"].(string), "pipeline_started") {
		t.Fatalf("expected events containing pipeline_started, got: %v", m["events"])
	}
}

func TestDeleteRun(t *testing.T) {
	h := newTestHandler(t)
	createTestTask(t, h, "delrun-task", "#!/bin/sh\nexit 0\n")
	p := createTestPipeline(t, h, "delrun-pipe")
	mustAddTask(t, h, p.ID, "delrun-task")

	runID, _ := startAndWait(t, h, p.ID, 10*time.Second)

	resp := doRequest(t, h, "DELETE", "/api/runs/"+runID, nil)
	mustStatus(t, resp, 200)

	resp = doRequest(t, h, "GET", "/api/runs/"+runID, nil)
	mustStatus(t, resp, 404)
}

func TestDeleteActiveRun(t *testing.T) {
	h := newTestHandler(t)
	createTestTask(t, h, "active-del", "#!/bin/sh\nsleep 10\necho done\n")
	p := createTestPipeline(t, h, "active-del-pipe")
	mustAddTask(t, h, p.ID, "active-del")

	resp := doRequest(t, h, "POST", "/api/pipelines/"+p.ID+"/start", nil)
	mustStatus(t, resp, 200)
	runID := decodeMap(t, resp)["run_id"].(string)
	time.Sleep(200 * time.Millisecond)

	resp = doRequest(t, h, "DELETE", "/api/runs/"+runID, nil)
	mustStatus(t, resp, 409)
	m := decodeMap(t, resp)
	if !strings.Contains(m["error"].(string), "active") {
		t.Fatalf("expected active run rejection, got: %v", m["error"])
	}

	waitForPipelineDone(t, h, p.ID, 15*time.Second)
}

// ===== State Tests =====

func TestGetStateIdle(t *testing.T) {
	h := newTestHandler(t)
	resp := doRequest(t, h, "GET", "/api/state", nil)
	mustStatus(t, resp, 200)
	s := decodeJSON[runner.OrchestratorState](t, resp)
	if len(s.RunningPipelines) != 0 {
		t.Fatalf("expected 0 running pipelines, got %d", len(s.RunningPipelines))
	}
}

func TestStateReflectsRunning(t *testing.T) {
	h := newTestHandler(t)
	createTestTask(t, h, "state-sleep", "#!/bin/sh\nsleep 5\necho awoken\n")
	p := createTestPipeline(t, h, "state-pipe")
	mustAddTask(t, h, p.ID, "state-sleep")

	doRequest(t, h, "POST", "/api/pipelines/"+p.ID+"/start", nil)
	time.Sleep(500 * time.Millisecond)

	// State should show running
	resp := doRequest(t, h, "GET", "/api/state", nil)
	mustStatus(t, resp, 200)
	s := decodeJSON[runner.OrchestratorState](t, resp)
	found := false
	for _, rp := range s.RunningPipelines {
		if rp.PipelineID == p.ID {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("expected pipeline to be in running_pipelines")
	}

	// Wait and verify it transitions to idle
	waitForPipelineDone(t, h, p.ID, 10*time.Second)
	resp = doRequest(t, h, "GET", "/api/state", nil)
	mustStatus(t, resp, 200)
	s = decodeJSON[runner.OrchestratorState](t, resp)
	for _, rp := range s.RunningPipelines {
		if rp.PipelineID == p.ID {
			t.Fatal("expected pipeline to be removed from running_pipelines after completion")
		}
	}
}

// ===== Loop Execution Test =====

func TestLoopExecution(t *testing.T) {
	h := newTestHandler(t)
	createTestTask(t, h, "loop-task", "#!/bin/sh\necho loop iteration\nexit 0\n")
	resp := doRequest(t, h, "POST", "/api/pipelines", map[string]interface{}{
		"name":       "loop-test",
		"loop_count": 2,
	})
	mustStatus(t, resp, 201)
	p := decodeJSON[pipeline.Pipeline](t, resp)
	mustAddTask(t, h, p.ID, "loop-task")

	startAndWait(t, h, p.ID, 15*time.Second)

	// Count runs for this pipeline
	resp = doRequest(t, h, "GET", "/api/runs?pipeline_id="+p.ID, nil)
	mustStatus(t, resp, 200)
	runs := decodeJSON[[]map[string]interface{}](t, resp)
	if len(runs) != 2 {
		t.Fatalf("expected 2 runs for loop_count=2, got %d", len(runs))
	}
	for _, r := range runs {
		runID := r["run_id"].(string)
		if !strings.HasPrefix(runID, "run-"+p.ID+"-") {
			t.Fatalf("expected run_id prefix run-%s-, got %s", p.ID, runID)
		}
	}
}

// ===== Cleanup Tests =====

func TestCleanupMaxRunsEnforcement(t *testing.T) {
	h := newTestHandler(t)
	createTestTask(t, h, "clean-task", "#!/bin/sh\nexit 0\n")
	p := createTestPipeline(t, h, "clean-pipe")
	mustAddTask(t, h, p.ID, "clean-task")

	// Run 5 times
	for i := 0; i < 5; i++ {
		doRequest(t, h, "POST", "/api/pipelines/"+p.ID+"/start", nil)
		time.Sleep(500 * time.Millisecond) // ensure different run IDs
	}
	// Wait for the last one to finish
	waitForPipelineDone(t, h, p.ID, 15*time.Second)

	// Verify 5 runs exist
	resp := doRequest(t, h, "GET", "/api/runs?pipeline_id="+p.ID, nil)
	mustStatus(t, resp, 200)
	runs := decodeJSON[[]map[string]interface{}](t, resp)
	if len(runs) != 5 {
		t.Fatalf("expected 5 runs before cleanup, got %d", len(runs))
	}

	// Enforce max 3
	deleted, _ := h.Runner.CleanupOldRuns(3)
	if deleted != 2 {
		t.Fatalf("expected 2 deleted (5 runs, limit 3), got %d", deleted)
	}

	resp = doRequest(t, h, "GET", "/api/runs?pipeline_id="+p.ID, nil)
	mustStatus(t, resp, 200)
	runs = decodeJSON[[]map[string]interface{}](t, resp)
	if len(runs) != 3 {
		t.Fatalf("expected 3 runs after cleanup, got %d", len(runs))
	}
}

func TestCleanupSkipsRunningPipelines(t *testing.T) {
	h := newTestHandler(t)
	createTestTask(t, h, "skip-clean", "#!/bin/sh\nexit 0\n")
	p := createTestPipeline(t, h, "skip-clean-pipe")
	mustAddTask(t, h, p.ID, "skip-clean")

	// Create 5 completed runs
	for i := 0; i < 5; i++ {
		doRequest(t, h, "POST", "/api/pipelines/"+p.ID+"/start", nil)
		time.Sleep(500 * time.Millisecond)
	}
	startAndWait(t, h, p.ID, 10*time.Second)

	// Now start a long run to mark as running
	createTestTask(t, h, "long-clean", "#!/bin/sh\nsleep 20\necho long\n")
	p2 := createTestPipeline(t, h, "running-clean")
	mustAddTask(t, h, p2.ID, "long-clean")
	doRequest(t, h, "POST", "/api/pipelines/"+p2.ID+"/start", nil)
	time.Sleep(500 * time.Millisecond)

	// Cleanup should skip p2 (running) and clean p1
	deleted, _ := h.Runner.CleanupOldRuns(3)
	t.Logf("deleted %d runs", deleted)

	// p1 should be cleaned (5→3 if limit=3, or 5→5 if skipped)
	resp := doRequest(t, h, "GET", "/api/runs?pipeline_id="+p.ID, nil)
	mustStatus(t, resp, 200)
	runs := decodeJSON[[]map[string]interface{}](t, resp)
	if len(runs) != 3 {
		t.Fatalf("expected p1 cleaned to 3 runs, got %d", len(runs))
	}

	// p2 should be untouched (running)
	resp = doRequest(t, h, "GET", "/api/runs?pipeline_id="+p2.ID, nil)
	mustStatus(t, resp, 200)
	runs2 := decodeJSON[[]map[string]interface{}](t, resp)
	if len(runs2) != 1 {
		t.Fatalf("expected p2 untouched (1 run), got %d", len(runs2))
	}

	// Clean up
	waitForPipelineDone(t, h, p2.ID, 30*time.Second)
}

// ===== Cron Validation Tests =====

func TestValidCronExpressions(t *testing.T) {
	h := newTestHandler(t)
	tests := []struct {
		schedule string
		want     int
	}{
		{"* * * * *", 201},
		{"*/5 * * * *", 201},
		{"0 9 * * 1-5", 201},
		{"0,30 * * * *", 201},
		{"5/10 * * * *", 201},
		{"0 0 1 1 *", 201},
		{"not cron", 400},
		{"* * * *", 400},
		{"60 * * * *", 400},
		{"* 24 * * *", 400},
	}
	for _, tt := range tests {
		t.Run("cron_"+tt.schedule, func(t *testing.T) {
			resp := doRequest(t, h, "POST", "/api/pipelines", map[string]interface{}{
				"name":     "cron-" + strconv.Itoa(int(time.Now().UnixNano())),
				"schedule": tt.schedule,
			})
			if resp.StatusCode != tt.want {
				body, _ := io.ReadAll(resp.Body)
				t.Fatalf("cron=%q expected %d, got %d: %s", tt.schedule, tt.want, resp.StatusCode, string(body))
			}
		})
	}
}

// ===== Duplicate Task Index Tests =====

func TestDuplicateTaskInPipeline(t *testing.T) {
	h := newTestHandler(t)
	createTestTask(t, h, "dup", "#!/bin/sh\necho dup\n")
	p := createTestPipeline(t, h, "dup-pipe")

	// Add the same task 3 times
	for i := 0; i < 3; i++ {
		mustAddTask(t, h, p.ID, "dup")
	}

	resp := doRequest(t, h, "GET", "/api/pipelines/"+p.ID, nil)
	mustStatus(t, resp, 200)
	m := decodeMap(t, resp)
	tasks := m["tasks"].([]interface{})
	if len(tasks) != 3 {
		t.Fatalf("expected 3 duplicate tasks, got %d", len(tasks))
	}

	// Configure each instance independently
	secs := 10
	doRequest(t, h, "PUT", "/api/pipelines/"+p.ID, map[string]interface{}{
		"action":          "set_task_config",
		"task_index":      0,
		"timeout_seconds": secs,
	})
	secs2 := 20
	doRequest(t, h, "PUT", "/api/pipelines/"+p.ID, map[string]interface{}{
		"action":          "set_task_config",
		"task_index":      2,
		"timeout_seconds": secs2,
	})

	// Verify independent configs
	resp = doRequest(t, h, "GET", "/api/pipelines/"+p.ID, nil)
	mustStatus(t, resp, 200)
	m = decodeMap(t, resp)
	tasks = m["tasks"].([]interface{})
	t0 := tasks[0].(map[string]interface{})
	t2 := tasks[2].(map[string]interface{})
	if t0["timeout_seconds"].(float64) != 10 {
		t.Fatalf("task[0] timeout: expected 10, got %v", t0["timeout_seconds"])
	}
	if t2["timeout_seconds"].(float64) != 20 {
		t.Fatalf("task[2] timeout: expected 20, got %v", t2["timeout_seconds"])
	}
	// Task[1] should have no override
	if tasks[1].(map[string]interface{})["timeout_seconds"] != nil {
		t.Fatalf("task[1] should have no timeout override")
	}
}

// ===== Pipeline Defaults Test =====

func TestRetryCountDefaults(t *testing.T) {
	h := newTestHandler(t)
	createTestTask(t, h, "retry-def", "#!/bin/sh\necho default\n")
	// Set task-level retry
	doRequest(t, h, "PUT", "/api/tasks/retry-def", map[string]interface{}{
		"run_command":     "./run.sh",
		"timeout_enabled": true,
		"timeout_seconds": 30,
		"retry_count":     2,
	})
	p := createTestPipeline(t, h, "retry-def-pipe")
	mustAddTask(t, h, p.ID, "retry-def")

	// Pipeline task should inherit retry_count=2 (nil override in pipeline)
	resp := doRequest(t, h, "GET", "/api/pipelines/"+p.ID, nil)
	mustStatus(t, resp, 200)
	m := decodeMap(t, resp)
	tasks := m["tasks"].([]interface{})
	t0 := tasks[0].(map[string]interface{})
	// Pipeline-level override is nil (inherit), so retry_count should be null in response
	if t0["retry_count"] != nil {
		t.Fatalf("expected nil (inherit) retry_count override, got %v", t0["retry_count"])
	}
}

// ===== Loop + ContinueRun Iteration Tests =====

func TestLoopContinueRunPreservesIteration(t *testing.T) {
	h := newTestHandler(t)
	createTestTask(t, h, "loop-conts", "#!/bin/sh\nsleep 2\nexit 0\n")

	resp := doRequest(t, h, "POST", "/api/pipelines", map[string]interface{}{
		"name":       "loop-cont-pipe",
		"loop_count": 3,
	})
	mustStatus(t, resp, 201)
	p := decodeJSON[pipeline.Pipeline](t, resp)
	mustAddTask(t, h, p.ID, "loop-conts")

	// Start and wait just past iteration 1 (task takes ~2s)
	doRequest(t, h, "POST", "/api/pipelines/"+p.ID+"/start", nil)
	time.Sleep(2500 * time.Millisecond)

	// Verify we're in iteration 2
	stateResp := doRequest(t, h, "GET", "/api/state", nil)
	mustStatus(t, stateResp, 200)
	state := decodeJSON[runner.OrchestratorState](t, stateResp)
	found := false
	var iter, total int
	for _, rp := range state.RunningPipelines {
		if rp.PipelineID == p.ID {
			iter = rp.Iteration
			total = rp.LoopTotal
			found = true
			break
		}
	}
	if !found {
		t.Fatal("pipeline should be running")
	}
	if iter != 2 || total != 3 {
		t.Fatalf("expected iteration=2 loop_total=3 during run, got iteration=%d loop_total=%d", iter, total)
	}

	// Stop pipeline
	doRequest(t, h, "POST", "/api/pipelines/"+p.ID+"/stop", nil)
	time.Sleep(500 * time.Millisecond)

	// Get the last run ID
	runsResp := doRequest(t, h, "GET", "/api/runs?pipeline_id="+p.ID, nil)
	mustStatus(t, runsResp, 200)
	runs := decodeJSON[[]map[string]interface{}](t, runsResp)
	if len(runs) == 0 {
		t.Fatal("expected at least 1 run")
	}
	lastRunID := runs[0]["run_id"].(string)

	// ContinueRun — should preserve iteration position
	resp = doRequest(t, h, "POST", "/api/runs/"+lastRunID+"/continue", map[string]interface{}{
		"pipeline_id": p.ID,
	})
	mustStatus(t, resp, 200)

	time.Sleep(200 * time.Millisecond)
	stateResp = doRequest(t, h, "GET", "/api/state", nil)
	mustStatus(t, stateResp, 200)
	state = decodeJSON[runner.OrchestratorState](t, stateResp)
	for _, rp := range state.RunningPipelines {
		if rp.PipelineID == p.ID {
			if rp.Iteration != 2 {
				t.Fatalf("ContinueRun should preserve iteration=2, got %d", rp.Iteration)
			}
			if rp.LoopTotal != 3 {
				t.Fatalf("ContinueRun should preserve loop_total=3, got %d", rp.LoopTotal)
			}
			break
		}
	}

	// Wait for remaining iterations to finish
	waitForPipelineDone(t, h, p.ID, 15*time.Second)

	// All 3 iterations should have completed
	runsResp = doRequest(t, h, "GET", "/api/runs?pipeline_id="+p.ID, nil)
	mustStatus(t, runsResp, 200)
	runs = decodeJSON[[]map[string]interface{}](t, runsResp)
	if len(runs) != 3 {
		t.Fatalf("expected 3 total runs after ContinueRun, got %d", len(runs))
	}
}

// ===== Webhook Tests =====

func TestWebhookSentOnCompletion(t *testing.T) {
	h := newTestHandler(t)

	received := make(chan map[string]interface{}, 1)
	webhookSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload map[string]interface{}
		if err := json.NewDecoder(r.Body).Decode(&payload); err == nil {
			received <- payload
		}
		w.WriteHeader(200)
	}))
	defer webhookSrv.Close()

	createTestTask(t, h, "wh-ok", "#!/bin/sh\nexit 0\n")
	resp := doRequest(t, h, "POST", "/api/pipelines", map[string]interface{}{
		"name":        "wh-pipe",
		"webhook_url": webhookSrv.URL,
	})
	mustStatus(t, resp, 201)
	p := decodeJSON[pipeline.Pipeline](t, resp)
	mustAddTask(t, h, p.ID, "wh-ok")

	startAndWait(t, h, p.ID, 10*time.Second)

	select {
	case payload := <-received:
		if payload["event"] != "pipeline_completed" {
			t.Fatalf("expected event pipeline_completed, got %v", payload["event"])
		}
		if payload["status"] != "success" {
			t.Fatalf("expected status success, got %v", payload["status"])
		}
		if payload["pipeline_id"] != p.ID {
			t.Fatalf("expected pipeline_id %s, got %v", p.ID, payload["pipeline_id"])
		}
	case <-time.After(5 * time.Second):
		t.Fatal("webhook was not received within timeout")
	}
}

func TestWebhookNotSentOnManualStop(t *testing.T) {
	h := newTestHandler(t)

	received := make(chan struct{}, 1)
	webhookSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received <- struct{}{}
		w.WriteHeader(200)
	}))
	defer webhookSrv.Close()

	createTestTask(t, h, "wh-stop", "#!/bin/sh\nsleep 30\necho done\n")
	resp := doRequest(t, h, "POST", "/api/pipelines", map[string]interface{}{
		"name":        "wh-stop-pipe",
		"webhook_url": webhookSrv.URL,
	})
	mustStatus(t, resp, 201)
	p := decodeJSON[pipeline.Pipeline](t, resp)
	mustAddTask(t, h, p.ID, "wh-stop")

	doRequest(t, h, "POST", "/api/pipelines/"+p.ID+"/start", nil)
	time.Sleep(500 * time.Millisecond)
	doRequest(t, h, "POST", "/api/pipelines/"+p.ID+"/stop", nil)
	time.Sleep(3 * time.Second)

	select {
	case <-received:
		t.Fatal("webhook should NOT be sent on manual stop")
	default:
		// expected — no webhook sent
	}
}

// ===== Stop Command Execution Test =====

func TestStopCommandExecuted(t *testing.T) {
	h := newTestHandler(t)

	// Create task whose stop_command writes a marker file
	markerFile := filepath.Join(t.TempDir(), "stop-executed")
	path := makeTar(t, "stopcmd-test", map[string]string{
		"run.sh":  "#!/bin/sh\nsleep 30\necho done\n",
		"stop.sh": "#!/bin/sh\ntouch '" + markerFile + "'\n",
	})
	resp := uploadTaskViaMultipart(t, h, path)
	mustStatus(t, resp, 201)

	p := createTestPipeline(t, h, "stopcmd-pipe")
	mustAddTask(t, h, p.ID, "stopcmd-test")

	doRequest(t, h, "POST", "/api/pipelines/"+p.ID+"/start", nil)
	time.Sleep(500 * time.Millisecond)
	doRequest(t, h, "POST", "/api/pipelines/"+p.ID+"/stop", nil)
	time.Sleep(3 * time.Second)

	if _, err := os.Stat(markerFile); os.IsNotExist(err) {
		t.Fatal("stop_command was not executed: marker file not found")
	}
}

// ===== Concurrent Pipeline Execution Test =====

func TestConcurrentPipelineExecution(t *testing.T) {
	h := newTestHandler(t)
	createTestTask(t, h, "conc-task", "#!/bin/sh\nsleep 2\nexit 0\n")

	p1 := createTestPipeline(t, h, "concurrent-a")
	mustAddTask(t, h, p1.ID, "conc-task")
	p2 := createTestPipeline(t, h, "concurrent-b")
	mustAddTask(t, h, p2.ID, "conc-task")

	// Start both concurrently
	respCh1 := make(chan *http.Response, 1)
	respCh2 := make(chan *http.Response, 1)
	go func() { respCh1 <- doRequest(t, h, "POST", "/api/pipelines/"+p1.ID+"/start", nil) }()
	go func() { respCh2 <- doRequest(t, h, "POST", "/api/pipelines/"+p2.ID+"/start", nil) }()

	r1 := <-respCh1
	r2 := <-respCh2
	if r1.StatusCode != 200 {
		t.Fatalf("pipeline-a start failed: %d", r1.StatusCode)
	}
	if r2.StatusCode != 200 {
		t.Fatalf("pipeline-b start failed: %d", r2.StatusCode)
	}

	// Verify state shows both running
	time.Sleep(100 * time.Millisecond)
	stateResp := doRequest(t, h, "GET", "/api/state", nil)
	mustStatus(t, stateResp, 200)
	state := decodeJSON[runner.OrchestratorState](t, stateResp)
	running := make(map[string]bool)
	for _, rp := range state.RunningPipelines {
		running[rp.PipelineID] = true
	}
	if !running[p1.ID] || !running[p2.ID] {
		t.Fatal("both pipelines should be running concurrently")
	}

	// Both should complete successfully
	waitForPipelineDone(t, h, p1.ID, 10*time.Second)
	waitForPipelineDone(t, h, p2.ID, 10*time.Second)

	// Verify both produced successful runs
	for _, pid := range []string{p1.ID, p2.ID} {
		resp := doRequest(t, h, "GET", "/api/runs?pipeline_id="+pid, nil)
		mustStatus(t, resp, 200)
		runs := decodeJSON[[]map[string]interface{}](t, resp)
		if len(runs) == 0 {
			t.Fatalf("pipeline %s should have at least 1 run", pid)
		}
		if runs[0]["status"] != "success" {
			t.Fatalf("pipeline %s expected success, got %v", pid, runs[0]["status"])
		}
	}
}

// ===== Loop Count Zero (Forever) Test =====

func TestLoopCountZeroRunsUntilStopped(t *testing.T) {
	h := newTestHandler(t)
	createTestTask(t, h, "forever-task", "#!/bin/sh\necho forever\nexit 0\n")

	resp := doRequest(t, h, "POST", "/api/pipelines", map[string]interface{}{
		"name":       "forever-loop",
		"loop_count": 0,
	})
	mustStatus(t, resp, 201)
	p := decodeJSON[pipeline.Pipeline](t, resp)
	mustAddTask(t, h, p.ID, "forever-task")

	doRequest(t, h, "POST", "/api/pipelines/"+p.ID+"/start", nil)

	// Let it run for a few iterations
	time.Sleep(2 * time.Second)

	// Stop it
	doRequest(t, h, "POST", "/api/pipelines/"+p.ID+"/stop", nil)
	time.Sleep(1 * time.Second)

	// Verify multiple runs were created
	runsResp := doRequest(t, h, "GET", "/api/runs?pipeline_id="+p.ID, nil)
	mustStatus(t, runsResp, 200)
	runs := decodeJSON[[]map[string]interface{}](t, runsResp)
	if len(runs) < 2 {
		t.Fatalf("expected at least 2 runs for loop_count=0, got %d", len(runs))
	}

	// Verify pipeline stopped (no longer running)
	stateResp := doRequest(t, h, "GET", "/api/state", nil)
	mustStatus(t, stateResp, 200)
	state := decodeJSON[runner.OrchestratorState](t, stateResp)
	for _, rp := range state.RunningPipelines {
		if rp.PipelineID == p.ID {
			t.Fatal("pipeline should not be running after stop")
		}
	}
}

// ===== Task Config Validation Tests =====

func TestUpdateTaskInvalidOnTimeout(t *testing.T) {
	h := newTestHandler(t)
	createTestTask(t, h, "val-task", "#!/bin/sh\necho ok\n")

	resp := doRequest(t, h, "PUT", "/api/tasks/val-task", map[string]interface{}{
		"on_timeout": "garbage",
	})
	if resp.StatusCode < 400 {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected error for invalid on_timeout, got %d: %s", resp.StatusCode, body)
	}
}

func TestUpdateTaskValidOnTimeout(t *testing.T) {
	h := newTestHandler(t)
	createTestTask(t, h, "val2-task", "#!/bin/sh\necho ok\n")

	for _, val := range []string{"skip", "fail", ""} {
		resp := doRequest(t, h, "PUT", "/api/tasks/val2-task", map[string]interface{}{
			"on_timeout": val,
		})
		mustStatus(t, resp, 200)
	}
}

// ===== Log Retrieval Edge Case Tests =====

func TestGetRunLogsInvalidTaskIdx(t *testing.T) {
	h := newTestHandler(t)
	createTestTask(t, h, "log-idx", "#!/bin/sh\necho ok\nexit 0\n")
	p := createTestPipeline(t, h, "log-idx-pipe")
	mustAddTask(t, h, p.ID, "log-idx")

	runID, _ := startAndWait(t, h, p.ID, 10*time.Second)

	resp := doRequest(t, h, "GET", "/api/runs/"+runID+"?log=1&task=log-idx&task_idx=abc", nil)
	if resp.StatusCode != 400 {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 400 for invalid task_idx, got %d: %s", resp.StatusCode, body)
	}
}

func TestGetRunEventsNonExistent(t *testing.T) {
	h := newTestHandler(t)
	resp := doRequest(t, h, "GET", "/api/runs/run-nonexist-000001/events", nil)
	mustStatus(t, resp, 404)
}

func TestDeleteRunInvalidID(t *testing.T) {
	h := newTestHandler(t)
	resp := doRequest(t, h, "DELETE", "/api/runs/not-a-run-id", nil)
	mustStatus(t, resp, 400)
	m := decodeMap(t, resp)
	if !strings.Contains(m["error"].(string), "invalid") {
		t.Fatalf("expected invalid run id error, got: %v", m["error"])
	}
}

func TestDeleteRunNonExistent(t *testing.T) {
	h := newTestHandler(t)
	resp := doRequest(t, h, "DELETE", "/api/runs/run-nonexist-000001", nil)
	mustStatus(t, resp, 404)
	m := decodeMap(t, resp)
	if !strings.Contains(m["error"].(string), "not found") {
		t.Fatalf("expected not found error, got: %v", m["error"])
	}
}

func TestGetRunInfoNonExistent(t *testing.T) {
	h := newTestHandler(t)
	resp := doRequest(t, h, "GET", "/api/runs/run-nonexist-000001", nil)
	mustStatus(t, resp, 404)
}

// ===== Integration: state transition & cross-component tests =====

func TestPipelineReorderAffectsExecution(t *testing.T) {
	h := newTestHandler(t)
	createTestTask(t, h, "task-order-1", "#!/bin/sh\necho 'task1'\n")
	createTestTask(t, h, "task-order-2", "#!/bin/sh\necho 'task2'\n")
	createTestTask(t, h, "task-order-3", "#!/bin/sh\necho 'task3'\n")

	p := createTestPipeline(t, h, "order-pipe")
	mustAddTask(t, h, p.ID, "task-order-1")
	mustAddTask(t, h, p.ID, "task-order-2")
	mustAddTask(t, h, p.ID, "task-order-3")

	// Reorder: reverse the order to [3,2,1]
	doRequest(t, h, "PUT", "/api/pipelines/"+p.ID, map[string]interface{}{
		"action":       "reorder",
		"task_indices": []int{2, 1, 0},
	})

	runID, instances := startAndWait(t, h, p.ID, 10*time.Second)
	_ = runID

	if len(instances) != 3 {
		t.Fatalf("expected 3 instances, got %d", len(instances))
	}
	// After reorder [2,1,0]: task-order-3 (index 2) first, task-order-2 (index 1) second, task-order-1 (index 0) third
	expectedOrder := []string{"task-order-3", "task-order-2", "task-order-1"}
	for i, inst := range instances {
		if inst.TaskName != expectedOrder[i] {
			t.Fatalf("position %d: expected %s, got %s", i, expectedOrder[i], inst.TaskName)
		}
		if inst.Status != "success" {
			t.Fatalf("position %d: expected success, got %s", i, inst.Status)
		}
	}
}

func TestTaskConfigPropagatesToExecution(t *testing.T) {
	h := newTestHandler(t)
	createTestTask(t, h, "config-prop", "#!/bin/sh\nsleep 3\necho done\n")

	// Set a short timeout (1s) so the task will definitely time out
	doRequest(t, h, "PUT", "/api/tasks/config-prop", map[string]interface{}{
		"run_command":     "./run.sh",
		"timeout_enabled": true,
		"timeout_seconds": 1,
		"on_timeout":      "skip",
	})

	p := createTestPipeline(t, h, "config-prop-pipe")
	mustAddTask(t, h, p.ID, "config-prop")

	_, instances := startAndWait(t, h, p.ID, 15*time.Second)
	if len(instances) != 1 {
		t.Fatalf("expected 1 instance, got %d", len(instances))
	}
	if instances[0].Status != "timeout" {
		t.Fatalf("expected timeout status with 1s timeout on 3s sleep, got %s", instances[0].Status)
	}

	// Now update to a longer timeout (10s) — task should complete
	doRequest(t, h, "PUT", "/api/tasks/config-prop", map[string]interface{}{
		"run_command":     "./run.sh",
		"timeout_enabled": true,
		"timeout_seconds": 10,
		"on_timeout":      "fail",
	})

	_, instances2 := startAndWait(t, h, p.ID, 15*time.Second)
	if len(instances2) != 1 {
		t.Fatalf("expected 1 instance, got %d", len(instances2))
	}
	if instances2[0].Status != "success" {
		t.Fatalf("expected success with 10s timeout on 3s sleep, got %s", instances2[0].Status)
	}
}

func TestDeletePipelineCleansUpRuns(t *testing.T) {
	h := newTestHandler(t)
	createTestTask(t, h, "cleanup-run", "#!/bin/sh\necho cleanup\n")
	p := createTestPipeline(t, h, "cleanup-run-pipe")
	mustAddTask(t, h, p.ID, "cleanup-run")

	runID, _ := startAndWait(t, h, p.ID, 10*time.Second)

	// Verify both pipeline and run exist
	resp := doRequest(t, h, "GET", "/api/pipelines/"+p.ID, nil)
	mustStatus(t, resp, 200)
	resp = doRequest(t, h, "GET", "/api/runs/"+runID, nil)
	mustStatus(t, resp, 200)

	// Delete pipeline — should also clean up associated runs
	resp = doRequest(t, h, "DELETE", "/api/pipelines/"+p.ID, nil)
	mustStatus(t, resp, 200)

	// Verify pipeline is gone
	resp = doRequest(t, h, "GET", "/api/pipelines/"+p.ID, nil)
	mustStatus(t, resp, 404)

	// Verify run is also cleaned up
	resp = doRequest(t, h, "GET", "/api/runs/"+runID, nil)
	mustStatus(t, resp, 404)
}

func TestFullLifecycleStateTransitions(t *testing.T) {
	h := newTestHandler(t)

	// 1. Idle state
	resp := doRequest(t, h, "GET", "/api/state", nil)
	mustStatus(t, resp, 200)
	s := decodeJSON[runner.OrchestratorState](t, resp)
	if len(s.RunningPipelines) != 0 {
		t.Fatalf("initial state should have 0 running pipelines, got %d", len(s.RunningPipelines))
	}

	// 2. Create and start
	createTestTask(t, h, "lifecycle-task", "#!/bin/sh\nsleep 2\necho life\n")
	p := createTestPipeline(t, h, "lifecycle-pipe")
	mustAddTask(t, h, p.ID, "lifecycle-task")

	doRequest(t, h, "POST", "/api/pipelines/"+p.ID+"/start", nil)
	time.Sleep(200 * time.Millisecond)

	// 3. Verify running state
	resp = doRequest(t, h, "GET", "/api/state", nil)
	mustStatus(t, resp, 200)
	s = decodeJSON[runner.OrchestratorState](t, resp)
	found := false
	for _, rp := range s.RunningPipelines {
		if rp.PipelineID == p.ID {
			found = true
			if rp.CurrentTask != "lifecycle-task" {
				t.Fatalf("expected current task lifecycle-task, got %s", rp.CurrentTask)
			}
			break
		}
	}
	if !found {
		t.Fatal("pipeline should be in running state")
	}

	// 4. Wait for completion
	waitForPipelineDone(t, h, p.ID, 10*time.Second)

	// 5. Verify idle state returned
	resp = doRequest(t, h, "GET", "/api/state", nil)
	mustStatus(t, resp, 200)
	s = decodeJSON[runner.OrchestratorState](t, resp)
	for _, rp := range s.RunningPipelines {
		if rp.PipelineID == p.ID {
			t.Fatal("pipeline should no longer be in state after completion")
		}
	}
}
