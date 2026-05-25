package runner

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ai-task-orchestrator/internal/task"
)

func TestWriteTaskMetaWithIndex(t *testing.T) {
	dir := t.TempDir()

	tests := []struct {
		name     string
		idx      int
		dirName  string
	}{
		{"task-A", 0, "task-A-0"},
		{"task-A", 1, "task-A-1"},
		{"task-B", 0, "task-B-0"},
		{"task-C", 99, "task-C-99"},
	}

	for _, tt := range tests {
		taskDir := filepath.Join(dir, tt.dirName)
		os.MkdirAll(taskDir, 0755)

		now := time.Now().UTC()
		if err := writeTaskMeta(taskDir, tt.name, "run-001", "pipeline-1",
			TaskStatusSuccess, &now, &now, 0, tt.idx); err != nil {
			t.Fatalf("writeTaskMeta(%s): %v", tt.dirName, err)
		}

		// Read back and verify the index
		data, err := os.ReadFile(filepath.Join(taskDir, "meta.json"))
		if err != nil {
			t.Fatalf("read meta.json for %s: %v", tt.dirName, err)
		}
		var inst TaskInstance
		if err := json.Unmarshal(data, &inst); err != nil {
			t.Fatalf("unmarshal meta.json for %s: %v", tt.dirName, err)
		}
		if inst.Index != tt.idx {
			t.Fatalf("index: expected %d, got %d", tt.idx, inst.Index)
		}
		if inst.TaskName != tt.name {
			t.Fatalf("task_name: expected %s, got %s", tt.name, inst.TaskName)
		}
	}
}

func TestRunInfoWithDuplicateTasks(t *testing.T) {
	dir := t.TempDir()
	runDir := filepath.Join(dir, "run-test")
	os.MkdirAll(runDir, 0755)

	// Simulate a run with task-echo-load at indices 0,1 and ai-security-auditer at index 2
	taskDirs := []struct {
		name string
		idx  int
		dir  string
	}{
		{"task-echo-load", 0, "task-echo-load-0"},
		{"task-echo-load", 1, "task-echo-load-1"},
		{"ai-security-auditer", 2, "ai-security-auditer-2"},
	}

	now := time.Now().UTC()
	for _, td := range taskDirs {
		d := filepath.Join(runDir, td.dir)
		os.MkdirAll(d, 0755)
		writeTaskMeta(d, td.name, "run-test", "pipeline-1",
			TaskStatusSuccess, &now, &now, 0, td.idx)
	}

	// Add task-data dirs that should be skipped
	os.MkdirAll(filepath.Join(runDir, "task-data-1"), 0755)
	os.MkdirAll(filepath.Join(runDir, "task-data-2"), 0755)

	// Test readInstancesFromDir
	instances := readInstancesFromDir(t, runDir)
	if len(instances) != 3 {
		t.Fatalf("expected 3 instances, got %d", len(instances))
	}

	// Verify all indices present
	indexCounts := make(map[int]int)
	for _, inst := range instances {
		indexCounts[inst.Index]++
	}
	for _, td := range taskDirs {
		if indexCounts[td.idx] != 1 {
			t.Fatalf("index %d: expected 1 instance, got %d", td.idx, indexCounts[td.idx])
		}
	}

	// Verify task names match
	nameMap := make(map[int]string)
	for _, inst := range instances {
		nameMap[inst.Index] = inst.TaskName
	}
	if nameMap[0] != "task-echo-load" || nameMap[1] != "task-echo-load" || nameMap[2] != "ai-security-auditer" {
		t.Fatalf("unexpected task names: %v", nameMap)
	}
}

func TestRunInfoSkipsTaskDataDirs(t *testing.T) {
	dir := t.TempDir()
	runDir := filepath.Join(dir, "run-skip")
	os.MkdirAll(runDir, 0755)

	// Create only task-data dirs (no real task dirs)
	os.MkdirAll(filepath.Join(runDir, "task-data-1"), 0755)
	os.MkdirAll(filepath.Join(runDir, "task-data-2"), 0755)

	instances := readInstancesFromDir(t, runDir)
	if len(instances) != 0 {
		t.Fatalf("expected 0 instances from task-data-only dir, got %d", len(instances))
	}
}

func TestRunInfoEmptyDir(t *testing.T) {
	dir := t.TempDir()
	runDir := filepath.Join(dir, "run-empty")
	os.MkdirAll(runDir, 0755)

	instances := readInstancesFromDir(t, runDir)
	if len(instances) != 0 {
		t.Fatalf("expected 0 instances from empty dir, got %d", len(instances))
	}
}

func TestRunInfoMixedTaskAndDataDirs(t *testing.T) {
	dir := t.TempDir()
	runDir := filepath.Join(dir, "run-mixed")
	os.MkdirAll(runDir, 0755)

	now := time.Now().UTC()

	// Real task dirs
	for i := 0; i < 3; i++ {
		d := filepath.Join(runDir, fmt.Sprintf("task-A-%d", i))
		os.MkdirAll(d, 0755)
		writeTaskMeta(d, "task-A", "run-mixed", "pipeline-1",
			TaskStatusSuccess, &now, &now, 0, i)
	}

	// task-data dirs
	os.MkdirAll(filepath.Join(runDir, "task-data-1"), 0755)
	os.MkdirAll(filepath.Join(runDir, "task-data-2"), 0755)

	// Extra non-task dir
	os.MkdirAll(filepath.Join(runDir, "other-dir"), 0755)

	instances := readInstancesFromDir(t, runDir)
	if len(instances) != 3 {
		t.Fatalf("expected 3 instances (excluding task-data and non-meta dirs), got %d", len(instances))
	}
}

func TestRunLogByIndex(t *testing.T) {
	dir := t.TempDir()
	runDir := filepath.Join(dir, "run-log")
	os.MkdirAll(runDir, 0755)

	// Create task-A at indices 0 and 1 with different content
	for idx := 0; idx < 2; idx++ {
		taskDir := filepath.Join(runDir, fmt.Sprintf("task-A-%d", idx))
		os.MkdirAll(taskDir, 0755)
		stdout := fmt.Sprintf("stdout for task-A index %d\n", idx)
		stderr := fmt.Sprintf("stderr for task-A index %d\n", idx)
		os.WriteFile(filepath.Join(taskDir, "stdout.log"), []byte(stdout), 0644)
		os.WriteFile(filepath.Join(taskDir, "stderr.log"), []byte(stderr), 0644)
	}

	// Read and verify each index
	for idx := 0; idx < 2; idx++ {
		stdout, stderr, err := readLogByPath(runDir, "task-A", idx)
		if err != nil {
			t.Fatalf("readLogByPath(idx=%d): %v", idx, err)
		}
		expectedStdout := fmt.Sprintf("stdout for task-A index %d\n", idx)
		expectedStderr := fmt.Sprintf("stderr for task-A index %d\n", idx)
		if stdout != expectedStdout {
			t.Fatalf("stdout idx=%d: expected %q, got %q", idx, expectedStdout, stdout)
		}
		if stderr != expectedStderr {
			t.Fatalf("stderr idx=%d: expected %q, got %q", idx, expectedStderr, stderr)
		}
	}

	// Non-existent index
	_, _, err := readLogByPath(runDir, "task-A", 99)
	if err == nil {
		t.Fatal("expected error for non-existent task index 99")
	}
}

func TestRunLogNonExistentRun(t *testing.T) {
	dir := t.TempDir()
	_, _, err := readLogByPath(dir, "no-such-task", 0)
	if err == nil {
		t.Fatal("expected error reading log from non-existent run")
	}
}

func TestRunLogSomeFilesMissing(t *testing.T) {
	dir := t.TempDir()
	runDir := filepath.Join(dir, "run-partial")
	os.MkdirAll(runDir, 0755)
	taskDir := filepath.Join(runDir, "task-A-0")
	os.MkdirAll(taskDir, 0755)

	// Only create stderr.log, no stdout.log
	os.WriteFile(filepath.Join(taskDir, "stderr.log"), []byte("only stderr"), 0644)

	stdout, stderr, err := readLogByPath(runDir, "task-A", 0)
	if err != nil {
		t.Fatal(err)
	}
	if stdout != "" {
		t.Fatalf("expected empty stdout, got %q", stdout)
	}
	if stderr != "only stderr" {
		t.Fatalf("expected 'only stderr', got %q", stderr)
	}
}

func TestMarkTaskWithIndex(t *testing.T) {
	dir := t.TempDir()
	taskDir := filepath.Join(dir, "test-mark-3")
	os.MkdirAll(taskDir, 0755)

	now := time.Now().UTC()
	writeTaskMeta(taskDir, "task-X", "run-mark", "pipeline-1", TaskStatusFailed, &now, &now, -1, 3)

	// Verify meta.json was written with correct index
	data, err := os.ReadFile(filepath.Join(taskDir, "meta.json"))
	if err != nil {
		t.Fatal(err)
	}
	var inst TaskInstance
	json.Unmarshal(data, &inst)
	if inst.Index != 3 {
		t.Fatalf("index: expected 3, got %d", inst.Index)
	}
	if inst.TaskName != "task-X" {
		t.Fatalf("task_name: expected task-X, got %s", inst.TaskName)
	}
	if inst.Status != TaskStatusFailed {
		t.Fatalf("status: expected %s, got %s", TaskStatusFailed, inst.Status)
	}
}

func TestComputeRunStatusWithDuplicates(t *testing.T) {
	tests := []struct {
		name     string
		statuses []string
		want     string
	}{
		{"all success",            []string{"success", "success"},              "success"},
		{"first success second running", []string{"success", "running"},        "running"},
		{"some failed",           []string{"success", "failed"},                "failed"},
		{"timeout then success",   []string{"timeout", "success"},              "success"},
		{"stopped",                []string{"stopped"},                          "failed"},
		{"pending",               []string{"pending", "success"},               "running"},
		{"all unknown",           []string{"crashed", "crashed"},               "failed"},
		{"empty",                 []string{},                                   "unknown"},
		{"single success",        []string{"success"},                          "success"},
		{"mixed failures",        []string{"failed", "timeout", "crashed", "stopped"}, "failed"},
		{"running with failures", []string{"running", "failed", "success"},     "running"},
		{"success then timeout",  []string{"success", "timeout"},               "failed"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			instances := make([]TaskInstance, len(tt.statuses))
			for i, s := range tt.statuses {
				instances[i] = TaskInstance{
					TaskName: fmt.Sprintf("task-%d", i),
					Status:   s,
					Index:    i,
				}
			}
			got := ComputeRunStatus(instances)
			if got != tt.want {
				t.Fatalf("expected %s, got %s", tt.want, got)
			}
		})
	}
}

// --- helpers for tests ---

func readInstancesFromDir(t *testing.T, runDir string) []TaskInstance {
	t.Helper()
	entries, err := os.ReadDir(runDir)
	if err != nil {
		t.Fatal(err)
	}
	var instances []TaskInstance
	for _, e := range entries {
		if !e.IsDir() || e.Name() == "task-data-1" || e.Name() == "task-data-2" {
			continue
		}
		f, err := os.Open(filepath.Join(runDir, e.Name(), "meta.json"))
		if err != nil {
			continue
		}
		var inst TaskInstance
		if json.NewDecoder(f).Decode(&inst) == nil {
			instances = append(instances, inst)
		}
		f.Close()
	}
	return instances
}

func readLogByPath(runsDir, taskName string, taskIdx int) (stdout, stderr string, err error) {
	taskDir := filepath.Join(runsDir, fmt.Sprintf("%s-%d", taskName, taskIdx))
	stdoutB, err1 := os.ReadFile(filepath.Join(taskDir, "stdout.log"))
	stderrB, err2 := os.ReadFile(filepath.Join(taskDir, "stderr.log"))
	if err1 != nil && err2 != nil {
		return "", "", err1
	}
	return string(stdoutB), string(stderrB), nil
}


// --- retry tests ---

func TestRetryOnTimeout(t *testing.T) {
	dir := t.TempDir()
	dataDir := filepath.Join(dir, "data")
	tasksDir := filepath.Join(dataDir, "tasks")
	taskMetaDir := filepath.Join(dataDir, "task_meta")
	pipelinesDir := filepath.Join(dataDir, "pipelines")
	runsDir := filepath.Join(dataDir, "runs")

	for _, d := range []string{tasksDir, taskMetaDir, pipelinesDir, runsDir} {
		os.MkdirAll(d, 0755)
	}

	// Create a task that sleeps
	taskName := "sleepy"
	taskDir := filepath.Join(tasksDir, taskName)
	os.MkdirAll(taskDir, 0755)
	os.WriteFile(filepath.Join(taskDir, "run.sh"), []byte("#!/bin/sh\nsleep 10\necho done\n"), 0755)

	// Write task meta
	meta := task.Meta{
		Name:           taskName,
		PackagePath:    "tasks/" + taskName,
		RunCommand:     "./run.sh",
		StopCommand:    "",
		TimeoutEnabled: true,
		TimeoutSeconds: 1,
		OnTimeout:      "fail",
		RetryCount:     1,
	}
	writeJSONFile(t, filepath.Join(taskMetaDir, taskName+".json"), meta)

	taskMgr := task.NewManager(tasksDir, taskMetaDir, pipelinesDir)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mgr := NewManager(runsDir, dataDir, taskMgr, logger)
	mgr.SetPipelineStatusSetter(&stubStatusSetter{})

	tasks := []RunTask{
		{Name: taskName, RetryCount: intPtr(1)},
	}
	runID, err := mgr.Start("pipeline-1", tasks, "", "test-pipeline")
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Wait for pipeline to finish (2 attempts, 1s timeout each + overhead)
	time.Sleep(5 * time.Second)

	// Verify retry entries in events log
	events, err := mgr.RunEvents(runID)
	if err != nil {
		t.Fatalf("RunEvents: %v", err)
	}
	if !strings.Contains(events, "event=retry") {
		t.Fatalf("expected retry event in events log, got:\n%s", events)
	}
	if !strings.Contains(events, "event=timeout") {
		t.Fatalf("expected timeout event in events log, got:\n%s", events)
	}

	// Verify final status is timeout (not success)
	instances, err := mgr.RunInfo(runID)
	if err != nil {
		t.Fatalf("RunInfo: %v", err)
	}
	if len(instances) != 1 {
		t.Fatalf("expected 1 instance, got %d", len(instances))
	}
	if instances[0].Status != TaskStatusTimeout {
		t.Fatalf("expected status timeout, got %s", instances[0].Status)
	}
}

func TestNoRetryOnNonTimeoutFailure(t *testing.T) {
	dir := t.TempDir()
	dataDir := filepath.Join(dir, "data")
	tasksDir := filepath.Join(dataDir, "tasks")
	taskMetaDir := filepath.Join(dataDir, "task_meta")
	pipelinesDir := filepath.Join(dataDir, "pipelines")
	runsDir := filepath.Join(dataDir, "runs")

	for _, d := range []string{tasksDir, taskMetaDir, pipelinesDir, runsDir} {
		os.MkdirAll(d, 0755)
	}

	// Create a task that exits non-zero immediately
	taskName := "failer"
	taskDir := filepath.Join(tasksDir, taskName)
	os.MkdirAll(taskDir, 0755)
	os.WriteFile(filepath.Join(taskDir, "run.sh"), []byte("#!/bin/sh\nexit 1\n"), 0755)

	meta := task.Meta{
		Name:           taskName,
		PackagePath:    "tasks/" + taskName,
		RunCommand:     "./run.sh",
		StopCommand:    "",
		TimeoutEnabled: false,
		RetryCount:     2,
	}
	writeJSONFile(t, filepath.Join(taskMetaDir, taskName+".json"), meta)

	taskMgr := task.NewManager(tasksDir, taskMetaDir, pipelinesDir)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mgr := NewManager(runsDir, dataDir, taskMgr, logger)
	mgr.SetPipelineStatusSetter(&stubStatusSetter{})

	tasks := []RunTask{
		{Name: taskName},
	}
	runID, err := mgr.Start("pipeline-1", tasks, "", "test-pipeline")
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	time.Sleep(2 * time.Second)

	// Verify NO retry events (retry only on timeout, not exit code)
	events, err := mgr.RunEvents(runID)
	if err != nil {
		t.Fatalf("RunEvents: %v", err)
	}
	if strings.Contains(events, "event=retry") {
		t.Fatalf("unexpected retry event for non-timeout failure:\n%s", events)
	}

	instances, err := mgr.RunInfo(runID)
	if err != nil {
		t.Fatalf("RunInfo: %v", err)
	}
	if instances[0].Status != TaskStatusFailed {
		t.Fatalf("expected status failed, got %s", instances[0].Status)
	}
}

func TestRetrySuccess(t *testing.T) {
	dir := t.TempDir()
	dataDir := filepath.Join(dir, "data")
	tasksDir := filepath.Join(dataDir, "tasks")
	taskMetaDir := filepath.Join(dataDir, "task_meta")
	pipelinesDir := filepath.Join(dataDir, "pipelines")
	runsDir := filepath.Join(dataDir, "runs")

	for _, d := range []string{tasksDir, taskMetaDir, pipelinesDir, runsDir} {
		os.MkdirAll(d, 0755)
	}

	// Script that fails on first run, succeeds on second
	// Uses a counter file to track attempts
	taskName := "flaky"
	taskDir := filepath.Join(tasksDir, taskName)
	os.MkdirAll(taskDir, 0755)
	// Use a flag file in the shared data dir: if it exists, succeed; else create it and timeout
	script := `#!/bin/sh
# flaky.sh: first run times out (sleep long), second run succeeds
MARKER="$TASK_DATA_READ/retry-marker"
if [ -f "$MARKER" ]; then
  echo "second attempt success"
  exit 0
fi
touch "$MARKER"
sleep 10
echo "first attempt timeout"
`
	os.WriteFile(filepath.Join(taskDir, "run.sh"), []byte(script), 0755)

	meta := task.Meta{
		Name:           taskName,
		PackagePath:    "tasks/" + taskName,
		RunCommand:     "./run.sh",
		StopCommand:    "",
		TimeoutEnabled: true,
		TimeoutSeconds: 1,
		OnTimeout:      "fail",
		RetryCount:     1,
	}
	writeJSONFile(t, filepath.Join(taskMetaDir, taskName+".json"), meta)

	taskMgr := task.NewManager(tasksDir, taskMetaDir, pipelinesDir)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mgr := NewManager(runsDir, dataDir, taskMgr, logger)
	mgr.SetPipelineStatusSetter(&stubStatusSetter{})

	tasks := []RunTask{
		{Name: taskName},
	}
	runID, err := mgr.Start("pipeline-1", tasks, "", "test-pipeline")
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	time.Sleep(5 * time.Second)

	events, err := mgr.RunEvents(runID)
	if err != nil {
		t.Fatalf("RunEvents: %v", err)
	}
	if !strings.Contains(events, "event=retry") {
		t.Fatalf("expected retry event, got:\n%s", events)
	}

	instances, err := mgr.RunInfo(runID)
	if err != nil {
		t.Fatalf("RunInfo: %v", err)
	}
	if instances[0].Status != TaskStatusSuccess {
		t.Fatalf("expected success after retry, got status=%s\nevents:\n%s", instances[0].Status, events)
	}
}

// --- helpers ---

func intPtr(v int) *int { return &v }

func writeJSONFile(t *testing.T, path string, v interface{}) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := json.NewEncoder(f).Encode(v); err != nil {
		t.Fatal(err)
	}
}

type stubStatusSetter struct{}

func (s *stubStatusSetter) SetStatus(id, status string) error { return nil }
