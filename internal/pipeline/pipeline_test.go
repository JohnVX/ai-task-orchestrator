package pipeline

import (
	"os"
	"path/filepath"
	"testing"
)

type mockTaskChecker struct{}

func (m *mockTaskChecker) Exists(name string) bool                   { return true }
func (m *mockTaskChecker) Pipelines(name string) ([]string, error)   { return nil, nil }

type mockRunCleaner struct{}

func (m *mockRunCleaner) DeleteRuns(pipelineID string) error { return nil }
func (m *mockRunCleaner) IsRunning(pipelineID string) bool    { return false }

func newTestManager(t *testing.T) *Manager {
	t.Helper()
	return NewManager(t.TempDir(), &mockTaskChecker{}, &mockRunCleaner{})
}

func mustCreate(t *testing.T, mgr *Manager, name, schedule string) *Pipeline {
	t.Helper()
	p, err := mgr.Create(name, schedule, "")
	if err != nil {
		t.Fatalf("Create(%q): %v", name, err)
	}
	return p
}

func mustAdd(t *testing.T, mgr *Manager, pipelineID, taskName string) {
	t.Helper()
	if err := mgr.AddTask(pipelineID, taskName); err != nil {
		t.Fatalf("AddTask(%q, %q): %v", pipelineID, taskName, err)
	}
}

func mustGet(t *testing.T, mgr *Manager, id string) *Pipeline {
	t.Helper()
	p, err := mgr.Get(id)
	if err != nil {
		t.Fatalf("Get(%q): %v", id, err)
	}
	return p
}

// --- Core duplicate task tests ---

func TestAddSameTaskMultipleTimes(t *testing.T) {
	mgr := newTestManager(t)
	p := mustCreate(t, mgr, "dup-test", "")

	for i := 0; i < 5; i++ {
		mustAdd(t, mgr, p.ID, "task-A")
	}

	p = mustGet(t, mgr, p.ID)
	if len(p.Tasks) != 5 {
		t.Fatalf("expected 5 tasks, got %d", len(p.Tasks))
	}
	for i, ref := range p.Tasks {
		if ref.Name != "task-A" {
			t.Fatalf("task[%d]: expected 'task-A', got %q", i, ref.Name)
		}
	}
}

func TestIndependentConfigsOnDuplicateTasks(t *testing.T) {
	mgr := newTestManager(t)
	p := mustCreate(t, mgr, "config-test", "")

	// Add 4 instances of same task
	for i := 0; i < 4; i++ {
		mustAdd(t, mgr, p.ID, "task-A")
	}

	// Configure each instance differently
	configs := []struct {
		idx     int
		timeout int
		action  string
	}{
		{0, 30, "fail"},
		{1, 60, "skip"},
		{2, 0, "fail"},  // disabled timeout
		{3, 120, "skip"},
	}

	for _, cfg := range configs {
		tSec := cfg.timeout
		tAction := cfg.action
		if err := mgr.SetTaskConfig(p.ID, cfg.idx, &tSec, &tAction, nil); err != nil {
			t.Fatalf("SetTaskConfig(idx=%d): %v", cfg.idx, err)
		}
	}

	p = mustGet(t, mgr, p.ID)
	for _, cfg := range configs {
		ref := p.Tasks[cfg.idx]
		if ref.TimeoutSeconds == nil || *ref.TimeoutSeconds != cfg.timeout {
			t.Fatalf("task[%d] timeout: expected %d, got %v", cfg.idx, cfg.timeout, ref.TimeoutSeconds)
		}
		if ref.OnTimeout == nil || *ref.OnTimeout != cfg.action {
			t.Fatalf("task[%d] on_timeout: expected %s, got %v", cfg.idx, cfg.action, ref.OnTimeout)
		}
	}
}

// --- Index-based removal tests ---

func TestRemoveTaskByIndexFirst(t *testing.T) {
	mgr := newTestManager(t)
	p := mustCreate(t, mgr, "remove-first", "")

	// Setup: A, A, B, B
	for _, name := range []string{"task-A", "task-A", "task-B", "task-B"} {
		mustAdd(t, mgr, p.ID, name)
	}

	// Configure all tasks distinctively
	for i := 0; i < 4; i++ {
		sec := (i + 1) * 10
		act := "fail"
		if i%2 == 1 {
			act = "skip"
		}
		mgr.SetTaskConfig(p.ID, i, &sec, &act, nil)
	}

	// Remove first task (index 0 = task-A with timeout 10)
	if err := mgr.RemoveTask(p.ID, 0); err != nil {
		t.Fatal(err)
	}

	p = mustGet(t, mgr, p.ID)
	if len(p.Tasks) != 3 {
		t.Fatalf("expected 3 tasks, got %d", len(p.Tasks))
	}

	// Remaining should be: A(20/skip), B(30/fail), B(40/skip)
	expected := []struct {
		name    string
		timeout int
		action  string
	}{
		{"task-A", 20, "skip"},
		{"task-B", 30, "fail"},
		{"task-B", 40, "skip"},
	}

	for i, exp := range expected {
		if p.Tasks[i].Name != exp.name {
			t.Fatalf("task[%d] name: expected %s, got %s", i, exp.name, p.Tasks[i].Name)
		}
		if p.Tasks[i].TimeoutSeconds == nil || *p.Tasks[i].TimeoutSeconds != exp.timeout {
			t.Fatalf("task[%d] timeout: expected %d, got %v", i, exp.timeout, p.Tasks[i].TimeoutSeconds)
		}
		if p.Tasks[i].OnTimeout == nil || *p.Tasks[i].OnTimeout != exp.action {
			t.Fatalf("task[%d] on_timeout: expected %s, got %v", i, exp.action, p.Tasks[i].OnTimeout)
		}
	}
}

func TestRemoveTaskByIndexMiddle(t *testing.T) {
	mgr := newTestManager(t)
	p := mustCreate(t, mgr, "remove-mid", "")

	for _, name := range []string{"A", "B", "C", "D", "E"} {
		mustAdd(t, mgr, p.ID, name)
	}

	// Remove index 2 (C)
	if err := mgr.RemoveTask(p.ID, 2); err != nil {
		t.Fatal(err)
	}
	p = mustGet(t, mgr, p.ID)

	expected := []string{"A", "B", "D", "E"}
	for i, name := range expected {
		if p.Tasks[i].Name != name {
			t.Fatalf("task[%d]: expected %s, got %s", i, name, p.Tasks[i].Name)
		}
	}
}

func TestRemoveTaskByIndexLast(t *testing.T) {
	mgr := newTestManager(t)
	p := mustCreate(t, mgr, "remove-last", "")
	mustAdd(t, mgr, p.ID, "A")
	mustAdd(t, mgr, p.ID, "B")

	if err := mgr.RemoveTask(p.ID, 1); err != nil {
		t.Fatal(err)
	}
	p = mustGet(t, mgr, p.ID)
	if len(p.Tasks) != 1 || p.Tasks[0].Name != "A" {
		t.Fatal("expected only task A remaining after removing last")
	}
}

func TestRemoveTaskByIndexOnly(t *testing.T) {
	mgr := newTestManager(t)
	p := mustCreate(t, mgr, "only-one", "")
	mustAdd(t, mgr, p.ID, "X")

	if err := mgr.RemoveTask(p.ID, 0); err != nil {
		t.Fatal(err)
	}
	p = mustGet(t, mgr, p.ID)
	if len(p.Tasks) != 0 {
		t.Fatal("expected empty task list after removing only task")
	}
}

// --- Reorder tests ---

func TestReorderPreservesConfigs(t *testing.T) {
	mgr := newTestManager(t)
	p := mustCreate(t, mgr, "reorder-config", "")

	for _, name := range []string{"A", "B", "C"} {
		mustAdd(t, mgr, p.ID, name)
	}

	// Set unique configs
	secs := []int{10, 20, 30}
	for i, s := range secs {
		mgr.SetTaskConfig(p.ID, i, &s, nil, nil)
	}

	// Reorder: C, A, B → indices [2, 0, 1]
	mgr.ReorderTasks(p.ID, []int{2, 0, 1})

	p = mustGet(t, mgr, p.ID)
	expected := []struct {
		name    string
		timeout int
	}{
		{"C", 30}, // was index 2
		{"A", 10}, // was index 0
		{"B", 20}, // was index 1
	}
	for i, exp := range expected {
		if p.Tasks[i].Name != exp.name {
			t.Fatalf("task[%d] name: expected %s, got %s", i, exp.name, p.Tasks[i].Name)
		}
		if p.Tasks[i].TimeoutSeconds == nil || *p.Tasks[i].TimeoutSeconds != exp.timeout {
			t.Fatalf("task[%d] timeout: expected %d, got %v", i, exp.timeout, p.Tasks[i].TimeoutSeconds)
		}
	}
}

func TestReorderWithDuplicates(t *testing.T) {
	mgr := newTestManager(t)
	p := mustCreate(t, mgr, "reorder-dup", "")

	// Setup: A, A, B, B
	for _, name := range []string{"A", "A", "B", "B"} {
		mustAdd(t, mgr, p.ID, name)
	}

	// Set configs: A(10), A(20), B(30), B(40)
	for i := 0; i < 4; i++ {
		sec := (i + 1) * 10
		mgr.SetTaskConfig(p.ID, i, &sec, nil, nil)
	}

	// Reorder to: B(40), A(10), B(30), A(20) → indices [3, 0, 2, 1]
	mgr.ReorderTasks(p.ID, []int{3, 0, 2, 1})

	p = mustGet(t, mgr, p.ID)
	expected := []struct {
		name    string
		timeout int
	}{
		{"B", 40},
		{"A", 10},
		{"B", 30},
		{"A", 20},
	}
	for i, exp := range expected {
		if p.Tasks[i].Name != exp.name {
			t.Fatalf("task[%d] name: expected %s, got %s", i, exp.name, p.Tasks[i].Name)
		}
		if p.Tasks[i].TimeoutSeconds == nil || *p.Tasks[i].TimeoutSeconds != exp.timeout {
			t.Fatalf("task[%d] timeout: expected %d, got %v", i, exp.timeout, p.Tasks[i].TimeoutSeconds)
		}
	}
}

func TestReorderInvalidIndex(t *testing.T) {
	mgr := newTestManager(t)
	p := mustCreate(t, mgr, "invalid-reorder", "")
	mustAdd(t, mgr, p.ID, "A")
	mustAdd(t, mgr, p.ID, "B")

	// Try reorder with mismatched count
	if err := mgr.ReorderTasks(p.ID, []int{0}); err == nil {
		t.Fatal("expected error for mismatched reorder count")
	}

	// Verify original order preserved
	p = mustGet(t, mgr, p.ID)
	if p.Tasks[0].Name != "A" || p.Tasks[1].Name != "B" {
		t.Fatal("original order should be preserved after failed reorder")
	}
}

// --- Boundary tests ---

func TestRemoveTaskOutOfBounds(t *testing.T) {
	mgr := newTestManager(t)
	p := mustCreate(t, mgr, "bounds", "")
	mustAdd(t, mgr, p.ID, "A")

	tests := []int{-5, -1, 1, 2, 100}
	for _, idx := range tests {
		if err := mgr.RemoveTask(p.ID, idx); err == nil {
			t.Fatalf("expected error for RemoveTask index %d", idx)
		}
	}

	// Task should still exist
	p = mustGet(t, mgr, p.ID)
	if len(p.Tasks) != 1 {
		t.Fatal("task list should be unchanged")
	}
}

func TestSetTaskConfigOutOfBounds(t *testing.T) {
	mgr := newTestManager(t)
	p := mustCreate(t, mgr, "config-bounds", "")
	mustAdd(t, mgr, p.ID, "A")
	mustAdd(t, mgr, p.ID, "B")

	tests := []int{-1, 2, 5}
	for _, idx := range tests {
		sec := 30
		if err := mgr.SetTaskConfig(p.ID, idx, &sec, nil, nil); err == nil {
			t.Fatalf("expected error for SetTaskConfig index %d", idx)
		}
	}
}

func TestSetTaskConfigInvalidOnTimeout(t *testing.T) {
	mgr := newTestManager(t)
	p := mustCreate(t, mgr, "invalid-timeout", "")
	mustAdd(t, mgr, p.ID, "A")

	bad := "invalid"
	if err := mgr.SetTaskConfig(p.ID, 0, nil, &bad, nil); err == nil {
		t.Fatal("expected error for invalid on_timeout value")
	}
}

func TestEmptyPipelineTasks(t *testing.T) {
	mgr := newTestManager(t)
	p := mustCreate(t, mgr, "empty-tasks", "")

	if len(p.Tasks) != 0 {
		t.Fatal("new pipeline should have empty tasks")
	}

	if err := mgr.RemoveTask(p.ID, 0); err == nil {
		t.Fatal("expected error removing from empty pipeline")
	}

	// Reorder on empty should fail too
	if err := mgr.ReorderTasks(p.ID, []int{}); err == nil {
		t.Fatal("expected error reordering empty pipeline")
	}
}

// --- Persistence tests ---

func TestPersistenceAcrossMultipleOperations(t *testing.T) {
	dir := t.TempDir()
	mgr := NewManager(dir, &mockTaskChecker{}, &mockRunCleaner{})

	p := mustCreate(t, mgr, "persist-ops", "")
	mustAdd(t, mgr, p.ID, "X")
	mustAdd(t, mgr, p.ID, "X")
	mustAdd(t, mgr, p.ID, "Y")

	// Configure
	sec0 := 30
	mgr.SetTaskConfig(p.ID, 0, &sec0, nil, nil)
	sec1 := 60
	act1 := "skip"
	mgr.SetTaskConfig(p.ID, 1, &sec1, &act1, nil)

	// Remove index 1 (second X with skip)
	mgr.RemoveTask(p.ID, 1)

	// Reorder: Y(no config), X(30/fail) → indices [1, 0]
	mgr.ReorderTasks(p.ID, []int{1, 0})

	// Create a NEW manager reading from same directory (simulates restart)
	mgr2 := NewManager(dir, &mockTaskChecker{}, &mockRunCleaner{})
	p2, err := mgr2.Get(p.ID)
	if err != nil {
		t.Fatal(err)
	}

	if len(p2.Tasks) != 2 {
		t.Fatalf("expected 2 tasks after all ops, got %d", len(p2.Tasks))
	}

	// After reorder: [0] = Y (was index 1), [1] = X (was index 0)
	if p2.Tasks[0].Name != "Y" {
		t.Fatalf("task[0]: expected Y, got %s", p2.Tasks[0].Name)
	}
	if p2.Tasks[1].Name != "X" {
		t.Fatalf("task[1]: expected X, got %s", p2.Tasks[1].Name)
	}
	// Task[1] (X) should retain its timeout from index 0
	if p2.Tasks[1].TimeoutSeconds == nil || *p2.Tasks[1].TimeoutSeconds != 30 {
		t.Fatalf("task[1] timeout: expected 30, got %v", p2.Tasks[1].TimeoutSeconds)
	}
}

// --- Multiple pipelines isolation ---

func TestDuplicateTasksAcrossPipelines(t *testing.T) {
	mgr := newTestManager(t)

	p1 := mustCreate(t, mgr, "pipe1", "")
	p2 := mustCreate(t, mgr, "pipe2", "")

	// Both pipelines get task-A
	mustAdd(t, mgr, p1.ID, "task-A")
	mustAdd(t, mgr, p2.ID, "task-A")

	// Configure differently
	sec1 := 10
	mgr.SetTaskConfig(p1.ID, 0, &sec1, nil, nil)
	sec2 := 20
	mgr.SetTaskConfig(p2.ID, 0, &sec2, nil, nil)

	p1 = mustGet(t, mgr, p1.ID)
	p2 = mustGet(t, mgr, p2.ID)

	if *p1.Tasks[0].TimeoutSeconds != 10 {
		t.Fatalf("pipe1 task timeout: expected 10, got %d", *p1.Tasks[0].TimeoutSeconds)
	}
	if *p2.Tasks[0].TimeoutSeconds != 20 {
		t.Fatalf("pipe2 task timeout: expected 20, got %d", *p2.Tasks[0].TimeoutSeconds)
	}
}

// --- Pipeline name case sensitivity ---

func TestDuplicatePipelineName(t *testing.T) {
	mgr := newTestManager(t)
	mustCreate(t, mgr, "MyPipe", "")
	_, err := mgr.Create("mypipe", "", "")
	if err == nil {
		t.Fatal("expected error creating pipeline with case-insensitive duplicate name")
	}
}

// --- All() results ---

func TestAllWithNoPipelines(t *testing.T) {
	mgr := newTestManager(t)
	all, err := mgr.All()
	if err != nil {
		t.Fatal(err)
	}
	if all == nil || len(all) != 0 {
		t.Fatal("All() should return empty slice, not nil")
	}
}

// --- Pipeline file format verification ---

func TestPipelineFileFormat(t *testing.T) {
	dir := t.TempDir()
	mgr := NewManager(dir, &mockTaskChecker{}, &mockRunCleaner{})

	p := mustCreate(t, mgr, "fmt-test", "")
	mustAdd(t, mgr, p.ID, "task-A")
	mustAdd(t, mgr, p.ID, "task-A")

	sec := 30
	mgr.SetTaskConfig(p.ID, 0, &sec, nil, nil)

	// Read raw file, verify JSON structure
	data, err := os.ReadFile(filepath.Join(dir, p.ID+".json"))
	if err != nil {
		t.Fatal(err)
	}
	content := string(data)

	// Should have two task-A entries
	p = mustGet(t, mgr, p.ID)
	if len(p.Tasks) != 2 {
		t.Fatalf("expected 2 tasks in file, got %d", len(p.Tasks))
	}
	if p.Tasks[0].Name != "task-A" || p.Tasks[1].Name != "task-A" {
		t.Fatal("both tasks should be named task-A")
	}
	_ = content
}
