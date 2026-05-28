package runner

import (
	"encoding/json"
	"fmt"
	"github.com/ai-task-orchestrator/internal/agent"
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
		name    string
		idx     int
		dirName string
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
		{"all success", []string{"success", "success"}, "success"},
		{"first success second running", []string{"success", "running"}, "running"},
		{"some failed", []string{"success", "failed"}, "failed"},
		{"timeout then success", []string{"timeout", "success"}, "success"},
		{"stopped", []string{"stopped"}, "failed"},
		{"pending", []string{"pending", "success"}, "running"},
		{"all unknown", []string{"crashed", "crashed"}, "failed"},
		{"empty", []string{}, "unknown"},
		{"single success", []string{"success"}, "success"},
		{"mixed failures", []string{"failed", "timeout", "crashed", "stopped"}, "failed"},
		{"running with failures", []string{"running", "failed", "success"}, "running"},
		{"success then timeout", []string{"success", "timeout"}, "failed"},
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
	mgr := NewManager(runsDir, dataDir, taskMgr, logger, agent.MustGet("claude-code"))
	mgr.SetPipelineStatusSetter(&stubStatusSetter{})

	tasks := []RunTask{
		{Name: taskName, RetryCount: intPtr(1)},
	}
	runID, err := mgr.Start("pipeline-1", tasks, "", "test-pipeline", 1)
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
	mgr := NewManager(runsDir, dataDir, taskMgr, logger, agent.MustGet("claude-code"))
	mgr.SetPipelineStatusSetter(&stubStatusSetter{})

	tasks := []RunTask{
		{Name: taskName},
	}
	runID, err := mgr.Start("pipeline-1", tasks, "", "test-pipeline", 1)
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
	mgr := NewManager(runsDir, dataDir, taskMgr, logger, agent.MustGet("claude-code"))
	mgr.SetPipelineStatusSetter(&stubStatusSetter{})

	tasks := []RunTask{
		{Name: taskName},
	}
	runID, err := mgr.Start("pipeline-1", tasks, "", "test-pipeline", 1)
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

// ===== resolveRemainingLoop =====

func TestResolveRemainingLoopNoFile(t *testing.T) {
	dir := t.TempDir()
	remaining, iteration, total := resolveRemainingLoop(dir)
	if remaining != 1 || iteration != 1 || total != 1 {
		t.Fatalf("expected (1,1,1) for missing file, got (%d,%d,%d)", remaining, iteration, total)
	}
}

func TestResolveRemainingLoopFirstIteration(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "iteration.json"), []byte(`{"iteration":1,"loop_total":5}`), 0644)
	remaining, iteration, total := resolveRemainingLoop(dir)
	if remaining != 5 || iteration != 1 || total != 5 {
		t.Fatalf("expected (5,1,5), got (%d,%d,%d)", remaining, iteration, total)
	}
}

func TestResolveRemainingLoopMidIteration(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "iteration.json"), []byte(`{"iteration":3,"loop_total":5}`), 0644)
	remaining, iteration, total := resolveRemainingLoop(dir)
	if remaining != 3 || iteration != 3 || total != 5 {
		t.Fatalf("expected (3,3,5), got (%d,%d,%d)", remaining, iteration, total)
	}
}

func TestResolveRemainingLoopLastIteration(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "iteration.json"), []byte(`{"iteration":5,"loop_total":5}`), 0644)
	remaining, iteration, total := resolveRemainingLoop(dir)
	if remaining != 1 || iteration != 5 || total != 5 {
		t.Fatalf("expected (1,5,5), got (%d,%d,%d)", remaining, iteration, total)
	}
}

func TestResolveRemainingLoopForever(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "iteration.json"), []byte(`{"iteration":7,"loop_total":0}`), 0644)
	remaining, iteration, total := resolveRemainingLoop(dir)
	if remaining != 0 || iteration != 7 || total != 0 {
		t.Fatalf("expected (0,7,0) for forever loop, got (%d,%d,%d)", remaining, iteration, total)
	}
}

func TestResolveRemainingLoopCorruptFile(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "iteration.json"), []byte(`garbage`), 0644)
	remaining, iteration, total := resolveRemainingLoop(dir)
	if remaining != 1 || iteration != 1 || total != 1 {
		t.Fatalf("expected (1,1,1) for corrupt file, got (%d,%d,%d)", remaining, iteration, total)
	}
}

func TestResolveRemainingLoopNegative(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "iteration.json"), []byte(`{"iteration":0,"loop_total":2}`), 0644)
	remaining, iteration, total := resolveRemainingLoop(dir)
	if remaining != 3 || iteration != 0 || total != 2 {
		t.Fatalf("expected (3,0,2), got (%d,%d,%d)", remaining, iteration, total)
	}
}

// ===== runSeq =====

func TestRunSeq(t *testing.T) {
	tests := []struct {
		runID string
		want  int
	}{
		{"run-pipeline-1-000001", 1},
		{"run-pipeline-1-000042", 42},
		{"run-p1-000999", 999},
		{"no-separator", 0},
		{"run-", 0},
		{"", 0},
		{"run-p-000000", 0},
		{"run-p-0000ab", 0},
	}
	for _, tt := range tests {
		got := runSeq(tt.runID)
		if got != tt.want {
			t.Errorf("runSeq(%q) = %d, want %d", tt.runID, got, tt.want)
		}
	}
}

// ===== DeleteRun =====

func TestDeleteRunUnit(t *testing.T) {
	dir := t.TempDir()
	dataDir := filepath.Join(dir, "data")
	runsDir := filepath.Join(dataDir, "runs")
	os.MkdirAll(runsDir, 0755)

	taskMgr := task.NewManager(filepath.Join(dataDir, "tasks"), filepath.Join(dataDir, "task_meta"), filepath.Join(dataDir, "pipelines"))
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mgr := NewManager(runsDir, dataDir, taskMgr, logger, agent.MustGet("claude-code"))

	// Non-existent run
	if err := mgr.DeleteRun("run-nonexist-000001"); err == nil {
		t.Fatal("expected error for non-existent run")
	}

	// Invalid run ID
	if err := mgr.DeleteRun("not-a-run"); err == nil {
		t.Fatal("expected error for invalid run ID")
	}
	if !strings.Contains(mgr.DeleteRun("not-a-run").Error(), "invalid") {
		t.Fatal("expected 'invalid' error message")
	}

	// Create a run with task data
	runDir := filepath.Join(runsDir, "run-p1-000001")
	os.MkdirAll(filepath.Join(runDir, "task-A-0"), 0755)
	now := time.Now().UTC()
	writeTaskMeta(filepath.Join(runDir, "task-A-0"), "task-A", "run-p1-000001", "pipeline-1", TaskStatusSuccess, &now, &now, 0, 0)

	// Delete it
	if err := mgr.DeleteRun("run-p1-000001"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, err := os.Stat(runDir); !os.IsNotExist(err) {
		t.Fatal("run dir should be removed after delete")
	}
}

// ===== RunDirSize =====

func TestRunDirSize(t *testing.T) {
	dir := t.TempDir()
	runsDir := filepath.Join(dir, "runs")
	os.MkdirAll(runsDir, 0755)

	taskMgr := task.NewManager(filepath.Join(dir, "tasks"), filepath.Join(dir, "task_meta"), filepath.Join(dir, "pipelines"))
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mgr := NewManager(runsDir, dir, taskMgr, logger, agent.MustGet("claude-code"))

	runDir := filepath.Join(runsDir, "run-p1-000001")
	os.MkdirAll(runDir, 0755)
	os.WriteFile(filepath.Join(runDir, "test.log"), []byte("hello"), 0644)

	size, err := mgr.RunDirSize("run-p1-000001")
	if err != nil {
		t.Fatal(err)
	}
	if size <= 0 {
		t.Fatalf("expected positive size, got %d", size)
	}

	// Non-existent run — walk swallows the error, returns 0 size
	size, err = mgr.RunDirSize("run-nonexist")
	if err != nil {
		t.Fatal(err)
	}
	if size != 0 {
		t.Fatalf("expected 0 size for non-existent run, got %d", size)
	}
}

// ===== helpers =====

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

func TestLoopExecution(t *testing.T) {
	dir := t.TempDir()
	dataDir := filepath.Join(dir, "data")
	tasksDir := filepath.Join(dataDir, "tasks")
	taskMetaDir := filepath.Join(dataDir, "task_meta")
	pipelinesDir := filepath.Join(dataDir, "pipelines")
	runsDir := filepath.Join(dataDir, "runs")

	for _, d := range []string{tasksDir, taskMetaDir, pipelinesDir, runsDir} {
		os.MkdirAll(d, 0755)
	}

	taskName := "quick"
	taskDir := filepath.Join(tasksDir, taskName)
	os.MkdirAll(taskDir, 0755)
	os.WriteFile(filepath.Join(taskDir, "run.sh"), []byte("#!/bin/sh\necho ok\nexit 0\n"), 0755)

	meta := task.Meta{
		Name:        taskName,
		PackagePath: "tasks/" + taskName,
		RunCommand:  "./run.sh",
	}
	writeJSONFile(t, filepath.Join(taskMetaDir, taskName+".json"), meta)

	taskMgr := task.NewManager(tasksDir, taskMetaDir, pipelinesDir)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mgr := NewManager(runsDir, dataDir, taskMgr, logger, agent.MustGet("claude-code"))
	mgr.SetPipelineStatusSetter(&stubStatusSetter{})

	tasks := []RunTask{{Name: taskName}}
	runID, err := mgr.Start("pipeline-1", tasks, "", "test-pipeline", 2)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	time.Sleep(1 * time.Second)

	// Check both iterations created runs
	entries, err := os.ReadDir(runsDir)
	if err != nil {
		t.Fatal(err)
	}
	runCount := 0
	for _, e := range entries {
		if e.IsDir() && strings.HasPrefix(e.Name(), "run-pipeline-1-") {
			runCount++
		}
	}
	if runCount != 2 {
		t.Fatalf("expected 2 runs for loop_count=2, got %d", runCount)
	}

	// Verify first run ID was returned
	if !strings.HasPrefix(runID, "run-pipeline-1-") {
		t.Fatalf("unexpected run ID: %s", runID)
	}
}

// ===== Crash Recovery Tests =====

func TestRecoverOnStartupMarksTasksCrashed(t *testing.T) {
	dir := t.TempDir()
	dataDir := filepath.Join(dir, "data")
	tasksDir := filepath.Join(dataDir, "tasks")
	taskMetaDir := filepath.Join(dataDir, "task_meta")
	pipelinesDir := filepath.Join(dataDir, "pipelines")
	runsDir := filepath.Join(dataDir, "runs")

	for _, d := range []string{tasksDir, taskMetaDir, pipelinesDir, runsDir} {
		os.MkdirAll(d, 0755)
	}

	// Create a task
	taskName := "recover-task"
	taskDir := filepath.Join(tasksDir, taskName)
	os.MkdirAll(taskDir, 0755)
	os.WriteFile(filepath.Join(taskDir, "run.sh"), []byte("#!/bin/sh\nsleep 30\nexit 0\n"), 0755)

	meta := task.Meta{
		Name:        taskName,
		PackagePath: "tasks/" + taskName,
		RunCommand:  "./run.sh",
	}
	writeJSONFile(t, filepath.Join(taskMetaDir, taskName+".json"), meta)

	taskMgr := task.NewManager(tasksDir, taskMetaDir, pipelinesDir)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mgr := NewManager(runsDir, dataDir, taskMgr, logger, agent.MustGet("claude-code"))
	mgr.SetPipelineStatusSetter(&stubStatusSetter{})

	// Create a run directory with a running task instance
	runDir := filepath.Join(runsDir, "run-p1-001")
	taskInstanceDir := filepath.Join(runDir, "recover-task-0")
	os.MkdirAll(taskInstanceDir, 0755)

	now := time.Now().UTC()
	writeTaskMeta(taskInstanceDir, taskName, "run-p1-001", "pipeline-1",
		TaskStatusRunning, &now, nil, -1, 0)

	// Write orchestrator_state.json with a dead PID
	statePath := filepath.Join(dataDir, "orchestrator_state.json")
	state := OrchestratorState{
		PID: 99999, // definitely not running
		RunningPipelines: []PipelineRunState{
			{
				PipelineID:   "pipeline-1",
				CurrentTask:  taskName,
				CurrentRunID: "run-p1-001",
				TaskIndex:    0,
				Iteration:    1,
				LoopTotal:    1,
			},
		},
	}
	writeJSONFile(t, statePath, state)

	// Run recovery
	if err := mgr.RecoverOnStartup(); err != nil {
		t.Fatalf("RecoverOnStartup: %v", err)
	}

	// Verify task was marked crashed
	data, err := os.ReadFile(filepath.Join(taskInstanceDir, "meta.json"))
	if err != nil {
		t.Fatal(err)
	}
	var inst TaskInstance
	if err := json.Unmarshal(data, &inst); err != nil {
		t.Fatal(err)
	}
	if inst.Status != TaskStatusCrashed {
		t.Fatalf("expected crashed status after recovery, got %s", inst.Status)
	}

	// Verify state file was cleared
	if _, err := os.Stat(statePath); !os.IsNotExist(err) {
		t.Fatal("orchestrator_state.json should be removed after recovery")
	}
}

func TestRecoverOnStartupRejectsAliveProcess(t *testing.T) {
	dir := t.TempDir()
	dataDir := filepath.Join(dir, "data")
	for _, d := range []string{"tasks", "task_meta", "pipelines", "runs"} {
		os.MkdirAll(filepath.Join(dataDir, d), 0755)
	}

	taskMgr := task.NewManager(filepath.Join(dataDir, "tasks"), filepath.Join(dataDir, "task_meta"), filepath.Join(dataDir, "pipelines"))
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mgr := NewManager(filepath.Join(dataDir, "runs"), dataDir, taskMgr, logger, agent.MustGet("claude-code"))

	// Write state with our own PID (alive)
	state := OrchestratorState{
		PID:              os.Getpid(),
		RunningPipelines: []PipelineRunState{{PipelineID: "p1", CurrentTask: "t", CurrentRunID: "r1"}},
	}
	writeJSONFile(t, filepath.Join(dataDir, "orchestrator_state.json"), state)

	err := mgr.RecoverOnStartup()
	if err == nil {
		t.Fatal("expected error when another instance is running")
	}
	if !strings.Contains(err.Error(), "another orchestrator instance is running") {
		t.Fatalf("expected 'another orchestrator instance is running', got: %v", err)
	}
}

// ===== Cron Matching Tests =====

func TestMatchCron(t *testing.T) {
	// Use a known time: Monday 2026-05-25 09:30:00 UTC
	ref := time.Date(2026, 5, 25, 9, 30, 0, 0, time.UTC)

	tests := []struct {
		expr    string
		matches bool
	}{
		// Wildcard
		{"* * * * *", true},
		// Exact minute match
		{"30 9 25 5 1", true},
		// Exact minute no-match
		{"31 9 25 5 1", false},
		// Range
		{"0-59 9 25 5 1", true},
		{"0-29 9 25 5 1", false}, // minute 30 not in range
		// Step
		{"*/5 * * * *", true},  // minute 30 divisible by 5
		{"*/7 * * * *", false}, // minute 30 not divisible by 7
		// N/step from start (minute 30: (30-5)%5==0)
		{"5/5 * * * *", true},
		// Comma list
		{"15,30,45 * * * *", true},
		{"15,45 * * * *", false},
		// Day of week
		{"30 9 * * 1", true},  // Monday
		{"30 9 * * 0", false}, // Sunday
	}

	for _, tt := range tests {
		t.Run("cron_"+strings.ReplaceAll(tt.expr, " ", "_"), func(t *testing.T) {
			result := MatchCron(tt.expr, ref)
			if result != tt.matches {
				t.Fatalf("MatchCron(%q, %s) = %v, want %v", tt.expr,
					ref.Format(time.RFC3339), result, tt.matches)
			}
		})
	}
}

func TestMatchCronStepFromNumber(t *testing.T) {
	// Regression test for H2 bug: N/step with start value
	// "5/5" means: start at 5, then every 5 mins → 5,10,15,20,25,30,35,40,45,50,55
	// Before H2 fix, value < start was incorrectly treated as a match because
	// the code used n != value instead of value < n

	// minute 3 < 5: below start, should not match
	ref := time.Date(2026, 5, 25, 9, 3, 0, 0, time.UTC)
	if MatchCron("5/5 * * * *", ref) {
		t.Fatal("minute 3 should not match 5/5 (value < start)")
	}
	// minute 7 > 5 but not on step (2%5 != 0), should not match
	ref = time.Date(2026, 5, 25, 9, 7, 0, 0, time.UTC)
	if MatchCron("5/5 * * * *", ref) {
		t.Fatal("minute 7 should not match 5/5 (not on step)")
	}
	// minute 25: (25-5)%5==0, should match
	ref = time.Date(2026, 5, 25, 9, 25, 0, 0, time.UTC)
	if !MatchCron("5/5 * * * *", ref) {
		t.Fatal("minute 25 should match 5/5: (25-5)%5==0")
	}
}

// ===== StopAll Test =====

func TestStopAllStopsRunningPipelines(t *testing.T) {
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
	taskName := "stopall-task"
	taskDir := filepath.Join(tasksDir, taskName)
	os.MkdirAll(taskDir, 0755)
	os.WriteFile(filepath.Join(taskDir, "run.sh"), []byte("#!/bin/sh\nsleep 30\nexit 0\n"), 0755)

	meta := task.Meta{
		Name:        taskName,
		PackagePath: "tasks/" + taskName,
		RunCommand:  "./run.sh",
	}
	writeJSONFile(t, filepath.Join(taskMetaDir, taskName+".json"), meta)

	taskMgr := task.NewManager(tasksDir, taskMetaDir, pipelinesDir)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mgr := NewManager(runsDir, dataDir, taskMgr, logger, agent.MustGet("claude-code"))
	mgr.SetPipelineStatusSetter(&stubStatusSetter{})

	// Start two pipelines
	tasks := []RunTask{{Name: taskName}}
	runID1, err := mgr.Start("pipeline-a", tasks, "", "pipe-a", 1)
	if err != nil {
		t.Fatalf("Start pipeline-a: %v", err)
	}
	runID2, err := mgr.Start("pipeline-b", tasks, "", "pipe-b", 1)
	if err != nil {
		t.Fatalf("Start pipeline-b: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	// Verify both are running
	if !mgr.IsRunning("pipeline-a") || !mgr.IsRunning("pipeline-b") {
		t.Fatal("both pipelines should be running")
	}

	// Stop all
	mgr.StopAll()
	time.Sleep(2 * time.Second)

	// Verify both stopped
	if mgr.IsRunning("pipeline-a") || mgr.IsRunning("pipeline-b") {
		t.Fatal("both pipelines should be stopped after StopAll")
	}

	// Verify both runs were marked stopped
	for _, rid := range []string{runID1, runID2} {
		instances, err := mgr.RunInfo(rid)
		if err != nil {
			t.Fatal(err)
		}
		if len(instances) == 0 {
			t.Fatalf("no instances for %s", rid)
		}
		s := instances[0].Status
		if s != TaskStatusStopped && s != TaskStatusPending {
			t.Fatalf("expected stopped or pending after StopAll for %s, got %s", rid, s)
		}
	}
}

func TestProcessStartTimeSelf(t *testing.T) {
	v := processStartTime(os.Getpid())
	if v == 0 {
		t.Fatal("processStartTime should return non-zero for current process")
	}
}

func TestProcessStartTimeInit(t *testing.T) {
	v := processStartTime(1)
	if v == 0 {
		t.Fatal("processStartTime should return non-zero for PID 1 (init)")
	}
}

func TestProcessStartTimeInvalidPID(t *testing.T) {
	v := processStartTime(99999999)
	if v != 0 {
		t.Fatalf("processStartTime should return 0 for invalid PID, got %d", v)
	}
}

func TestProcessStartTimeNegativePID(t *testing.T) {
	v := processStartTime(-1)
	if v != 0 {
		t.Fatalf("processStartTime should return 0 for negative PID, got %d", v)
	}
}

func TestComputeStages(t *testing.T) {
	tests := []struct {
		name  string
		tasks []RunTask
		want  [][]int // expected grouping: each inner slice = indices in a stage
	}{
		{
			name:  "all empty",
			tasks: []RunTask{{Name: "A"}, {Name: "B"}, {Name: "C"}},
			want:  [][]int{{0}, {1}, {2}},
		},
		{
			name: "two task stage",
			tasks: []RunTask{
				{Name: "A", Stage: "build"},
				{Name: "B", Stage: "build"},
			},
			want: [][]int{{0, 1}},
		},
		{
			name: "mixed with empties",
			tasks: []RunTask{
				{Name: "A"},
				{Name: "B", Stage: "build"},
				{Name: "C", Stage: "build"},
				{Name: "D"},
			},
			want: [][]int{{0}, {1, 2}, {3}},
		},
		{
			name: "single named stage",
			tasks: []RunTask{
				{Name: "A", Stage: "solo"},
				{Name: "B"},
			},
			want: [][]int{{0}, {1}},
		},
		{
			name: "non-adjacent same name",
			tasks: []RunTask{
				{Name: "A", Stage: "x"},
				{Name: "B"},
				{Name: "C", Stage: "x"},
			},
			want: [][]int{{0}, {1}, {2}},
		},
		{
			name: "three stages",
			tasks: []RunTask{
				{Name: "A", Stage: "s1"},
				{Name: "B", Stage: "s1"},
				{Name: "C", Stage: "s2"},
				{Name: "D", Stage: "s2"},
			},
			want: [][]int{{0, 1}, {2, 3}},
		},
		{
			name:  "single task",
			tasks: []RunTask{{Name: "only"}},
			want:  [][]int{{0}},
		},
		{
			name: "all same stage",
			tasks: []RunTask{
				{Name: "A", Stage: "x"},
				{Name: "B", Stage: "x"},
				{Name: "C", Stage: "x"},
			},
			want: [][]int{{0, 1, 2}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stages := computeStages(tt.tasks)
			if len(stages) != len(tt.want) {
				t.Fatalf("expected %d stages, got %d", len(tt.want), len(stages))
			}
			for si, wantIndices := range tt.want {
				got := make([]int, len(stages[si].tasks))
				for ti, st := range stages[si].tasks {
					got[ti] = st.index
				}
				if len(got) != len(wantIndices) {
					t.Fatalf("stage %d: expected %d tasks, got %d", si, len(wantIndices), len(got))
				}
				for i := range got {
					if got[i] != wantIndices[i] {
						t.Fatalf("stage %d: expected indices %v, got %v", si, wantIndices, got)
					}
				}
			}
		})
	}
}
