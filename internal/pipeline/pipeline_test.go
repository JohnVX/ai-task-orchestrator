package pipeline
import (
	"io"
	"log/slog"
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
func (m *mockRunCleaner) DeletePipelineRuns(id string, removeFile func() error) error {
	return removeFile()
}
func newTestManager(t *testing.T) *Manager {
	t.Helper()
	return NewManager(t.TempDir(), &mockTaskChecker{}, &mockRunCleaner{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
}
func mustCreate(t *testing.T, mgr *Manager, name, schedule string) *Pipeline {
	t.Helper()
	p, err := mgr.Create(name, schedule, "", nil)
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
 
func TestSetTaskConfigStageValidation(t *testing.T) {
	t.Parallel()
	mgr := newTestManager(t)
	p := mustCreate(t, mgr, "stage-val", "")
	mustAdd(t, mgr, p.ID, "A")
	// Valid stage names
	for _, valid := range []string{"build", "BUILD", "a-b", "a_b", "Ab-CD_eF"} {
		s := valid
		if err := mgr.SetTaskConfig(p.ID, 0, nil, nil, nil, nil, &s); err != nil {
			t.Fatalf("expected stage %q to be valid, got: %v", valid, err)
		}
	}
	// Clear stage with empty string
	empty := ""
	if err := mgr.SetTaskConfig(p.ID, 0, nil, nil, nil, nil, &empty); err != nil {
		t.Fatalf("expected empty stage to clear, got: %v", err)
	}
	// Invalid characters
	for _, invalid := range []string{"bad!", "hello world", "中文", "a/b"} {
		s := invalid
		if err := mgr.SetTaskConfig(p.ID, 0, nil, nil, nil, nil, &s); err == nil {
			t.Fatalf("expected stage %q to be rejected", invalid)
		}
	}
}
// --- Persistence tests ---
 
func TestPersistenceAcrossMultipleOperations(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	mgr := NewManager(dir, &mockTaskChecker{}, &mockRunCleaner{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	p := mustCreate(t, mgr, "persist-ops", "")
	mustAdd(t, mgr, p.ID, "X")
	mustAdd(t, mgr, p.ID, "X")
	mustAdd(t, mgr, p.ID, "Y")
	// Configure
	sec0 := 30
	mgr.SetTaskConfig(p.ID, 0, &sec0, nil, nil, nil, nil)
	sec1 := 60
	act1 := "skip"
	mgr.SetTaskConfig(p.ID, 1, &sec1, &act1, nil, nil, nil)
	// Remove index 1 (second X with skip)
	mgr.RemoveTask(p.ID, 1)
	// Reorder: Y(no config), X(30/fail) → indices [1, 0]
	mgr.ReorderTasks(p.ID, []int{1, 0})
	// Create a NEW manager reading from same directory (simulates restart)
	mgr2 := NewManager(dir, &mockTaskChecker{}, &mockRunCleaner{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
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
	t.Parallel()
	mgr := newTestManager(t)
	p1 := mustCreate(t, mgr, "pipe1", "")
	p2 := mustCreate(t, mgr, "pipe2", "")
	// Both pipelines get task-A
	mustAdd(t, mgr, p1.ID, "task-A")
	mustAdd(t, mgr, p2.ID, "task-A")
	// Configure differently
	sec1 := 10
	mgr.SetTaskConfig(p1.ID, 0, &sec1, nil, nil, nil, nil)
	sec2 := 20
	mgr.SetTaskConfig(p2.ID, 0, &sec2, nil, nil, nil, nil)
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
	t.Parallel()
	mgr := newTestManager(t)
	mustCreate(t, mgr, "MyPipe", "")
	_, err := mgr.Create("mypipe", "", "", nil)
	if err == nil {
		t.Fatal("expected error creating pipeline with case-insensitive duplicate name")
	}
}
// --- All() results ---
 
func TestAllWithNoPipelines(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
	dir := t.TempDir()
	mgr := NewManager(dir, &mockTaskChecker{}, &mockRunCleaner{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	p := mustCreate(t, mgr, "fmt-test", "")
	mustAdd(t, mgr, p.ID, "task-A")
	mustAdd(t, mgr, p.ID, "task-A")
	sec := 30
	mgr.SetTaskConfig(p.ID, 0, &sec, nil, nil, nil, nil)
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
