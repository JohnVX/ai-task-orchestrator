package runner

import (
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

func TestCleanupOldRunsDisabled(t *testing.T) {
	t.Parallel()
	m := setupCleanupManager(t)
	createRuns(t, m.runsDir, "pipeline-1", 5)

	deleted, freed := m.CleanupOldRuns(0)
	if deleted != 0 || freed != 0 {
		t.Fatalf("maxRuns=0 should disable cleanup, got deleted=%d freed=%d", deleted, freed)
	}

	assertRunCount(t, m.runsDir, "pipeline-1", 5)
}

func TestCleanupOldRunsWithinLimit(t *testing.T) {
	t.Parallel()
	m := setupCleanupManager(t)
	createRuns(t, m.runsDir, "pipeline-1", 3)

	deleted, _ := m.CleanupOldRuns(5)
	if deleted != 0 {
		t.Fatalf("expected 0 deleted for 3 runs with limit 5, got %d", deleted)
	}
	assertRunCount(t, m.runsDir, "pipeline-1", 3)
}

func TestCleanupOldRunsExactlyAtLimit(t *testing.T) {
	t.Parallel()
	m := setupCleanupManager(t)
	createRuns(t, m.runsDir, "pipeline-1", 5)

	deleted, _ := m.CleanupOldRuns(5)
	if deleted != 0 {
		t.Fatalf("expected 0 deleted for exactly 5 runs with limit 5, got %d", deleted)
	}
	assertRunCount(t, m.runsDir, "pipeline-1", 5)
}

func TestCleanupOldRunsOverLimit(t *testing.T) {
	t.Parallel()
	m := setupCleanupManager(t)
	createRuns(t, m.runsDir, "pipeline-1", 10)

	deleted, _ := m.CleanupOldRuns(3)
	if deleted != 7 {
		t.Fatalf("expected 7 deleted (10 runs, limit 3), got %d", deleted)
	}
	assertRunCount(t, m.runsDir, "pipeline-1", 3)

	// Verify the 3 newest runs survived: run-pipeline-1-008, -009, -010
	for i := 8; i <= 10; i++ {
		runID := fmt.Sprintf("run-pipeline-1-%06d", i)
		assertRunExists(t, m.runsDir, runID)
	}
	// Verify the 7 oldest are gone
	for i := 1; i <= 7; i++ {
		runID := fmt.Sprintf("run-pipeline-1-%06d", i)
		assertRunNotExists(t, m.runsDir, runID)
	}
}

func TestCleanupOldRunsSkipsRunningPipeline(t *testing.T) {
	t.Parallel()
	m := setupCleanupManager(t)
	createRuns(t, m.runsDir, "pipeline-1", 10)

	// Mark pipeline-1 as running in state
	m.stateMu.Lock()
	state := &OrchestratorState{
		PID: os.Getpid(),
		RunningPipelines: []PipelineRunState{
			{PipelineID: "pipeline-1", CurrentTask: "task-A", CurrentRunID: "run-pipeline-1-010"},
		},
	}
	m.writeState(state)
	m.stateMu.Unlock()

	deleted, _ := m.CleanupOldRuns(3)
	if deleted != 0 {
		t.Fatalf("expected 0 deleted for running pipeline, got %d", deleted)
	}
	assertRunCount(t, m.runsDir, "pipeline-1", 10)

	// Clean up state
	m.clearState()
}

func TestCleanupOldRunsMultiplePipelines(t *testing.T) {
	t.Parallel()
	m := setupCleanupManager(t)
	createRuns(t, m.runsDir, "pipeline-1", 8)
	createRuns(t, m.runsDir, "pipeline-2", 3)

	deleted, _ := m.CleanupOldRuns(5)
	if deleted != 3 {
		t.Fatalf("expected 3 deleted (pipeline-1: 3 over limit, pipeline-2: 0), got %d", deleted)
	}
	assertRunCount(t, m.runsDir, "pipeline-1", 5)
	assertRunCount(t, m.runsDir, "pipeline-2", 3)

	// pipeline-1: runs 001-003 deleted, 004-008 remain
	for i := 1; i <= 3; i++ {
		runID := fmt.Sprintf("run-pipeline-1-%06d", i)
		assertRunNotExists(t, m.runsDir, runID)
	}
	for i := 4; i <= 8; i++ {
		runID := fmt.Sprintf("run-pipeline-1-%06d", i)
		assertRunExists(t, m.runsDir, runID)
	}
}

func TestCleanupOldRunsMixedWithRunning(t *testing.T) {
	t.Parallel()
	m := setupCleanupManager(t)
	createRuns(t, m.runsDir, "pipeline-1", 10)
	createRuns(t, m.runsDir, "pipeline-2", 6)

	// pipeline-1 is running — skip
	// pipeline-2 is idle — clean to 3
	m.stateMu.Lock()
	state := &OrchestratorState{
		PID: os.Getpid(),
		RunningPipelines: []PipelineRunState{
			{PipelineID: "pipeline-1", CurrentTask: "task-A", CurrentRunID: "run-pipeline-1-010"},
		},
	}
	m.writeState(state)
	m.stateMu.Unlock()

	deleted, _ := m.CleanupOldRuns(3)
	if deleted != 3 {
		t.Fatalf("expected 3 deleted (only pipeline-2 cleaned), got %d", deleted)
	}
	assertRunCount(t, m.runsDir, "pipeline-1", 10) // untouched
	assertRunCount(t, m.runsDir, "pipeline-2", 3)  // cleaned

	m.clearState()
}

func TestCleanupOldRunsNoRuns(t *testing.T) {
	t.Parallel()
	m := setupCleanupManager(t)
	// No runs created

	deleted, freed := m.CleanupOldRuns(100)
	if deleted != 0 || freed != 0 {
		t.Fatalf("expected 0,0 for empty runs dir, got deleted=%d freed=%d", deleted, freed)
	}
}

func TestCleanupOldRunsNonRunDirsIgnored(t *testing.T) {
	t.Parallel()
	m := setupCleanupManager(t)
	// Create a non-run directory
	os.MkdirAll(filepath.Join(m.runsDir, "not-a-run"), 0755)
	createRuns(t, m.runsDir, "pipeline-1", 2)

	deleted, _ := m.CleanupOldRuns(100)
	if deleted != 0 {
		t.Fatalf("non-run dirs should be ignored, got %d deleted", deleted)
	}
}

// --- helpers ---

func setupCleanupManager(t *testing.T) *Manager {
	t.Helper()
	dir := t.TempDir()
	dataDir := filepath.Join(dir, "data")
	runsDir := filepath.Join(dataDir, "runs")
	tasksDir := filepath.Join(dataDir, "tasks")
	taskMetaDir := filepath.Join(dataDir, "task_meta")
	pipelinesDir := filepath.Join(dataDir, "pipelines")

	for _, d := range []string{runsDir, tasksDir, taskMetaDir, pipelinesDir} {
		if err := os.MkdirAll(d, 0755); err != nil {
			t.Fatal(err)
		}
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	taskMgr := task.NewManager(tasksDir, taskMetaDir, pipelinesDir, logger)
	return NewManager(runsDir, dataDir, taskMgr, logger, agent.MustGet("claude-code"))
}

// createRuns creates n run directories for the given pipeline, each with a
// single success meta.json so RunInfo() can identify the pipeline.
func createRuns(t *testing.T, runsDir, pipelineID string, n int) {
	t.Helper()
	now := time.Now().UTC()
	for i := 1; i <= n; i++ {
		runID := fmt.Sprintf("run-%s-%06d", pipelineID, i)
		runDir := filepath.Join(runsDir, runID)
		taskDir := filepath.Join(runDir, "task-A-0")
		if err := os.MkdirAll(taskDir, 0755); err != nil {
			t.Fatal(err)
		}
		if err := writeTaskMeta(taskDir, "task-A", runID, pipelineID, TaskStatusSuccess, &now, &now, 0, 0); err != nil {
			t.Fatal(err)
		}
	}
}

func assertRunCount(t *testing.T, runsDir, pipelineID string, want int) {
	t.Helper()
	entries, err := os.ReadDir(runsDir)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	prefix := "run-" + pipelineID + "-"
	for _, e := range entries {
		if e.IsDir() && strings.HasPrefix(e.Name(), prefix) {
			// Check it actually has a meta.json (is a real run)
			metaPath := filepath.Join(runsDir, e.Name(), "task-A-0", "meta.json")
			if _, err := os.Stat(metaPath); err == nil {
				count++
			}
		}
	}
	if count != want {
		t.Fatalf("expected %d runs for %s, got %d", want, pipelineID, count)
	}
}

func assertRunExists(t *testing.T, runsDir, runID string) {
	t.Helper()
	runDir := filepath.Join(runsDir, runID)
	if _, err := os.Stat(runDir); os.IsNotExist(err) {
		t.Fatalf("expected run %s to exist, but it was deleted", runID)
	}
}

func assertRunNotExists(t *testing.T, runsDir, runID string) {
	t.Helper()
	runDir := filepath.Join(runsDir, runID)
	if _, err := os.Stat(runDir); err == nil {
		t.Fatalf("expected run %s to be deleted, but it still exists", runID)
	}
}
