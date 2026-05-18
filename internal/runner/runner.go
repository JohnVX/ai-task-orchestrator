package runner

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/ai-task-orchestrator/internal/task"
)

// Task instance statuses within a run.
const (
	TaskStatusPending = "pending"
	TaskStatusRunning = "running"
	TaskStatusSuccess = "success"
	TaskStatusFailed  = "failed"
	TaskStatusStopped = "stopped"
	TaskStatusCrashed = "crashed"
)

// TaskInstance records the result of a single task execution within a run.
type TaskInstance struct {
	TaskName   string     `json:"task_name"`
	RunID      string     `json:"run_id"`
	PipelineID string     `json:"pipeline_id"`
	Status     string     `json:"status"`
	StartedAt  *time.Time `json:"started_at"`
	EndedAt    *time.Time `json:"ended_at"`
	ExitCode   int        `json:"exit_code"`
}

// PipelineRunState tracks a single running pipeline within the state file.
type PipelineRunState struct {
	PipelineID   string `json:"pipeline_id"`
	CurrentTask  string `json:"current_task"`
	CurrentRunID string `json:"current_run_id"`
}

// OrchestratorState is the global state persisted to orchestrator_state.json.
// When PID is non-zero it acts as a single-instance lock. RunningPipelines
// tracks all currently executing pipelines for crash recovery.
type OrchestratorState struct {
	PID              int                `json:"pid"`
	RunningPipelines []PipelineRunState `json:"running_pipelines"`
}

// PipelineStatusSetter updates pipeline status (set by runner during execution).
type PipelineStatusSetter interface {
	SetStatus(id, status string) error
}

// runControl holds per-pipeline execution state, protected by Manager.mu.
type runControl struct {
	cmd    *exec.Cmd
	stopCh chan struct{}
}

// Manager handles run lifecycle: start, stop, dual-buffer management.
type Manager struct {
	runsDir        string
	dataDir        string
	taskMgr        *task.Manager
	pipelineStatus PipelineStatusSetter
	logger         *slog.Logger

	mu      sync.Mutex
	running map[string]*runControl // pipelineID → control
}

// NewManager creates a Manager. It ensures the runs directory exists.
func NewManager(runsDir, dataDir string, taskMgr *task.Manager, logger *slog.Logger) *Manager {
	os.MkdirAll(runsDir, 0755)
	return &Manager{runsDir: runsDir, dataDir: dataDir, taskMgr: taskMgr, logger: logger, running: make(map[string]*runControl)}
}

// SetPipelineStatusSetter sets the pipeline status updater (wired after construction).
func (m *Manager) SetPipelineStatusSetter(ps PipelineStatusSetter) {
	m.pipelineStatus = ps
}

// --- helpers ---

func (m *Manager) statePath() string {
	return filepath.Join(m.dataDir, "orchestrator_state.json")
}

func (m *Manager) readState() (*OrchestratorState, error) {
	f, err := os.Open(m.statePath())
	if err != nil {
		if os.IsNotExist(err) {
			return &OrchestratorState{}, nil
		}
		return nil, err
	}
	defer f.Close()
	var s OrchestratorState
	if err := json.NewDecoder(f).Decode(&s); err != nil {
		return &OrchestratorState{}, nil
	}
	return &s, nil
}

func (m *Manager) writeState(s *OrchestratorState) error {
	f, err := os.Create(m.statePath())
	if err != nil {
		return err
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	return enc.Encode(s)
}

func (m *Manager) clearState() {
	os.Remove(m.statePath())
}

func (m *Manager) nextRunID() string {
	entries, _ := os.ReadDir(m.runsDir)
	maxN := 0
	for _, e := range entries {
		rest := strings.TrimPrefix(e.Name(), "run-")
		if n, err := strconv.Atoi(rest); err == nil && n > maxN {
			maxN = n
		}
	}
	return fmt.Sprintf("run-%03d", maxN+1)
}

func clearDir(path string) error {
	if err := os.RemoveAll(path); err != nil {
		return err
	}
	return os.MkdirAll(path, 0755)
}

func writeTaskMeta(runDir, taskName, runID, pipelineID, status string, startedAt, endedAt *time.Time, exitCode int) error {
	inst := TaskInstance{
		TaskName:   taskName,
		RunID:      runID,
		PipelineID: pipelineID,
		Status:     status,
		StartedAt:  startedAt,
		EndedAt:    endedAt,
		ExitCode:   exitCode,
	}
	f, err := os.Create(filepath.Join(runDir, taskName, "meta.json"))
	if err != nil {
		return err
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	return enc.Encode(inst)
}

// appendEvent writes a line to the per-run events log. Best-effort only.
func (m *Manager) appendEvent(runID, format string, args ...any) {
	f, err := os.OpenFile(filepath.Join(m.runsDir, runID, "events.log"),
		os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return
	}
	defer f.Close()
	fmt.Fprintf(f, format+"\n", args...)
}

// --- public methods ---

// Start begins pipeline execution. Multiple pipelines can run concurrently.
func (m *Manager) Start(pipelineID string, tasks []string) (runID string, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.running[pipelineID]; exists {
		return "", fmt.Errorf("pipeline %q is already running", pipelineID)
	}

	runID = m.nextRunID()
	runDir := filepath.Join(m.runsDir, runID)

	os.MkdirAll(filepath.Join(runDir, "task-data-1"), 0755)
	os.MkdirAll(filepath.Join(runDir, "task-data-2"), 0755)
	for _, t := range tasks {
		os.MkdirAll(filepath.Join(runDir, t), 0755)
	}

	state, _ := m.readState()
	if state.PID == 0 {
		state.PID = os.Getpid()
	}
	state.RunningPipelines = append(state.RunningPipelines, PipelineRunState{
		PipelineID:   pipelineID,
		CurrentTask:  tasks[0],
		CurrentRunID: runID,
	})
	if err := m.writeState(state); err != nil {
		return "", fmt.Errorf("write state: %w", err)
	}

	m.pipelineStatus.SetStatus(pipelineID, "running")
	m.logger.Info("pipeline started", "pipeline_id", pipelineID, "run_id", runID)
	m.appendEvent(runID, "%s pipeline=%s event=pipeline_started", time.Now().UTC().Format(time.RFC3339), pipelineID)

	ctl := &runControl{stopCh: make(chan struct{})}
	m.running[pipelineID] = ctl

	go m.runLoop(pipelineID, runID, runDir, tasks, ctl)

	return runID, nil
}

func (m *Manager) runLoop(pipelineID, runID, runDir string, tasks []string, ctl *runControl) {
	defer func() {
		m.removeFromState(pipelineID)
		m.logger.Info("pipeline finished", "pipeline_id", pipelineID, "run_id", runID)
		m.appendEvent(runID, "%s pipeline=%s event=pipeline_finished", time.Now().UTC().Format(time.RFC3339), pipelineID)
		m.pipelineStatus.SetStatus(pipelineID, "idle")
		m.mu.Lock()
		delete(m.running, pipelineID)
		m.mu.Unlock()
	}()

	for i, taskName := range tasks {
		select {
		case <-ctl.stopCh:
			m.markTask(runDir, taskName, runID, pipelineID, TaskStatusPending, nil)
			m.logger.Info("task status changed", "run_id", runID, "task", taskName, "status", TaskStatusPending)
			m.appendEvent(runID, "%s task=%s status=%s", time.Now().UTC().Format(time.RFC3339), taskName, TaskStatusPending)
			return
		default:
		}

		meta, err := m.taskMgr.Get(taskName)
		if err != nil {
			m.markTask(runDir, taskName, runID, pipelineID, TaskStatusFailed, nil)
			m.logger.Info("task status changed", "run_id", runID, "task", taskName, "status", TaskStatusFailed)
			m.appendEvent(runID, "%s task=%s status=%s", time.Now().UTC().Format(time.RFC3339), taskName, TaskStatusFailed)
			return
		}

		m.updateState(pipelineID, taskName, runID)

		var writeBuf, readBuf string
		if i%2 == 0 {
			writeBuf, readBuf = "task-data-1", "task-data-2"
		} else {
			writeBuf, readBuf = "task-data-2", "task-data-1"
		}

		if err := clearDir(filepath.Join(runDir, writeBuf)); err != nil {
			m.logger.Warn("clear write buffer", "dir", writeBuf, "error", err)
		}

		now := time.Now().UTC()
		writeTaskMeta(runDir, taskName, runID, pipelineID, TaskStatusRunning, &now, nil, -1)
		m.logger.Info("task status changed", "run_id", runID, "task", taskName, "status", TaskStatusRunning)
		m.appendEvent(runID, "%s task=%s status=%s", time.Now().UTC().Format(time.RFC3339), taskName, TaskStatusRunning)

		cmd := exec.Command("sh", "-c", meta.RunCommand)
		cmd.Dir = filepath.Join(m.dataDir, meta.PackagePath)
		cmd.Env = append(os.Environ(),
			"TASK_DATA_READ="+filepath.Join(runDir, readBuf),
			"TASK_DATA_WRITE="+filepath.Join(runDir, writeBuf),
			"TASK_DATA_1="+filepath.Join(runDir, "task-data-1"),
			"TASK_DATA_2="+filepath.Join(runDir, "task-data-2"),
		)

		stdoutF, cerr := os.Create(filepath.Join(runDir, taskName, "stdout.log"))
		if cerr != nil {
			m.logger.Warn("create stdout log", "error", cerr)
		}
		stderrF, cerr := os.Create(filepath.Join(runDir, taskName, "stderr.log"))
		if cerr != nil {
			m.logger.Warn("create stderr log", "error", cerr)
		}
		cmd.Stdout = stdoutF
		cmd.Stderr = stderrF

		m.mu.Lock()
		ctl.cmd = cmd
		m.mu.Unlock()

		if err := cmd.Start(); err != nil {
			m.mu.Lock()
			ctl.cmd = nil
			m.mu.Unlock()
			if stdoutF != nil {
				stdoutF.Close()
			}
			if stderrF != nil {
				stderrF.Close()
			}
			endedAt := time.Now().UTC()
			writeTaskMeta(runDir, taskName, runID, pipelineID, TaskStatusFailed, &now, &endedAt, -1)
			m.logger.Error("task start failed", "task", taskName, "error", err)
			m.appendEvent(runID, "%s task=%s status=%s error=%s", time.Now().UTC().Format(time.RFC3339), taskName, TaskStatusFailed, err)
			return
		}

		waitDone := make(chan error, 1)
		go func() { waitDone <- cmd.Wait() }()

		var execErr error
		select {
		case execErr = <-waitDone:
			// normal completion
		case <-ctl.stopCh:
			m.runStopCommand(meta)
			cmd.Process.Signal(syscall.SIGTERM)
			select {
			case execErr = <-waitDone:
			case <-time.After(10 * time.Second):
				cmd.Process.Kill()
				execErr = <-waitDone
			}
		}

		m.mu.Lock()
		ctl.cmd = nil
		m.mu.Unlock()

		if stdoutF != nil {
			stdoutF.Close()
		}
		if stderrF != nil {
			stderrF.Close()
		}

		endedAt := time.Now().UTC()

		if execErr != nil {
			exitCode := -1
			if exitErr, ok := execErr.(*exec.ExitError); ok {
				exitCode = exitErr.ExitCode()
			}
			status := TaskStatusFailed
			select {
			case <-ctl.stopCh:
				status = TaskStatusStopped
			default:
			}
			writeTaskMeta(runDir, taskName, runID, pipelineID, status, &now, &endedAt, exitCode)
			m.logger.Info("task status changed", "run_id", runID, "task", taskName, "status", status, "exit_code", exitCode)
			m.appendEvent(runID, "%s task=%s status=%s exit_code=%d", time.Now().UTC().Format(time.RFC3339), taskName, status, exitCode)
			return
		}

		writeTaskMeta(runDir, taskName, runID, pipelineID, TaskStatusSuccess, &now, &endedAt, 0)
		m.logger.Info("task status changed", "run_id", runID, "task", taskName, "status", TaskStatusSuccess)
		m.appendEvent(runID, "%s task=%s status=%s", time.Now().UTC().Format(time.RFC3339), taskName, TaskStatusSuccess)

		clearDir(filepath.Join(runDir, readBuf))
	}
}

func (m *Manager) runStopCommand(meta *task.Meta) {
	stopCmd := meta.StopCommand
	if stopCmd == "" {
		return
	}
	cmd := exec.Command("sh", "-c", stopCmd)
	cmd.Dir = filepath.Join(m.dataDir, meta.PackagePath)
	done := make(chan struct{})
	go func() { cmd.Run(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		cmd.Process.Kill()
	}
}

func (m *Manager) updateState(pipelineID, taskName, runID string) {
	state, _ := m.readState()
	for i, rp := range state.RunningPipelines {
		if rp.PipelineID == pipelineID {
			state.RunningPipelines[i].CurrentTask = taskName
			break
		}
	}
	m.writeState(state)
}

func (m *Manager) markTask(runDir, taskName, runID, pipelineID, status string, startedAt *time.Time) {
	endedAt := time.Now().UTC()
	if startedAt == nil {
		startedAt = &endedAt
	}
	writeTaskMeta(runDir, taskName, runID, pipelineID, status, startedAt, &endedAt, -1)
}

// Stop halts a specific running pipeline.
func (m *Manager) Stop(pipelineID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	ctl, ok := m.running[pipelineID]
	if !ok {
		return fmt.Errorf("pipeline %q is not running", pipelineID)
	}
	select {
	case <-ctl.stopCh:
		return fmt.Errorf("pipeline %q is already stopping", pipelineID)
	default:
		close(ctl.stopCh)
	}
	return nil
}

// IsRunning returns true if the given pipeline is currently executing.
func (m *Manager) IsRunning(pipelineID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.running[pipelineID]
	return ok
}

// State returns the current orchestrator global state.
func (m *Manager) State() (*OrchestratorState, error) {
	return m.readState()
}

// RunInfo returns metadata about a specific run.
func (m *Manager) RunInfo(runID string) ([]TaskInstance, error) {
	runDir := filepath.Join(m.runsDir, runID)
	entries, err := os.ReadDir(runDir)
	if err != nil {
		return nil, err
	}
	var instances []TaskInstance
	for _, e := range entries {
		if !e.IsDir() || strings.HasPrefix(e.Name(), "task-data-") {
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
	return instances, nil
}

// RunLog returns stdout/stderr content for a task within a run.
func (m *Manager) RunLog(runID, taskName string) (stdout, stderr string, err error) {
	runDir := filepath.Join(m.runsDir, runID, taskName)
	stdoutB, err1 := os.ReadFile(filepath.Join(runDir, "stdout.log"))
	stderrB, err2 := os.ReadFile(filepath.Join(runDir, "stderr.log"))
	if err1 != nil && err2 != nil {
		return "", "", fmt.Errorf("no logs found for %s in %s", taskName, runID)
	}
	return string(stdoutB), string(stderrB), nil
}

// RunEvents returns the content of the per-run events log.
func (m *Manager) RunEvents(runID string) (string, error) {
	data, err := os.ReadFile(filepath.Join(m.runsDir, runID, "events.log"))
	if err != nil {
		if os.IsNotExist(err) {
			return "", nil
		}
		return "", err
	}
	return string(data), nil
}

// removeFromState removes a single pipeline from the persisted state.
// If no running pipelines remain, the state file is deleted.
func (m *Manager) removeFromState(pipelineID string) {
	state, _ := m.readState()
	if state == nil {
		return
	}
	updated := make([]PipelineRunState, 0, len(state.RunningPipelines))
	for _, rp := range state.RunningPipelines {
		if rp.PipelineID != pipelineID {
			updated = append(updated, rp)
		}
	}
	if len(updated) == 0 {
		m.clearState()
	} else {
		state.RunningPipelines = updated
		m.writeState(state)
	}
}

// DeleteRun removes a single run directory.
func (m *Manager) DeleteRun(runID string) error {
	if !strings.HasPrefix(runID, "run-") {
		return fmt.Errorf("invalid run id %q", runID)
	}
	runDir := filepath.Join(m.runsDir, runID)
	if _, err := os.Stat(runDir); err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("run %q not found", runID)
		}
		return fmt.Errorf("stat run dir: %w", err)
	}

	m.mu.Lock()
	state, _ := m.readState()
	active := false
	for _, rp := range state.RunningPipelines {
		if rp.CurrentRunID == runID {
			active = true
			break
		}
	}
	m.mu.Unlock()
	if active {
		return fmt.Errorf("cannot delete active run %q", runID)
	}

	if err := os.RemoveAll(runDir); err != nil {
		return fmt.Errorf("remove run dir: %w", err)
	}
	m.logger.Info("run deleted", "run_id", runID)
	return nil
}

// DeleteRuns removes all run data for a pipeline.
func (m *Manager) DeleteRuns(pipelineID string) error {
	entries, err := os.ReadDir(m.runsDir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if !e.IsDir() || !strings.HasPrefix(e.Name(), "run-") {
			continue
		}
		runDir := filepath.Join(m.runsDir, e.Name())
		instances, _ := m.RunInfo(e.Name())
		for _, inst := range instances {
			if inst.PipelineID == pipelineID {
				os.RemoveAll(runDir)
				break
			}
		}
	}
	return nil
}

// RunDirSize returns the total size of a run directory in bytes.
func (m *Manager) RunDirSize(runID string) (int64, error) {
	runDir := filepath.Join(m.runsDir, runID)
	var size int64
	err := filepath.Walk(runDir, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return nil
	})
	return size, err
}

// RecoverOnStartup checks the lock file PID, cleans up stale locks for all pipelines.
func (m *Manager) RecoverOnStartup() error {
	state, err := m.readState()
	if err != nil {
		return err
	}
	if state.PID == 0 || len(state.RunningPipelines) == 0 {
		return nil
	}

	alive := pidAlive(state.PID)
	if alive {
		return fmt.Errorf("another orchestrator instance is running (PID %d)", state.PID)
	}

	for _, ps := range state.RunningPipelines {
		runDir := filepath.Join(m.runsDir, ps.CurrentRunID)
		os.MkdirAll(filepath.Join(runDir, ps.CurrentTask), 0755)
		now := time.Now().UTC()
		writeTaskMeta(runDir, ps.CurrentTask, ps.CurrentRunID, ps.PipelineID, TaskStatusCrashed, nil, &now, -1)
		m.logger.Info("task status changed", "run_id", ps.CurrentRunID, "task", ps.CurrentTask, "status", TaskStatusCrashed, "reason", "stale_lock")
		m.appendEvent(ps.CurrentRunID, "%s task=%s status=%s reason=stale_lock", time.Now().UTC().Format(time.RFC3339), ps.CurrentTask, TaskStatusCrashed)

		entries, err := os.ReadDir(runDir)
		if err == nil {
			for _, e := range entries {
				if !e.IsDir() || strings.HasPrefix(e.Name(), "task-data-") {
					continue
				}
				metaPath := filepath.Join(runDir, e.Name(), "meta.json")
				f, err := os.Open(metaPath)
				if err != nil {
					continue
				}
				var inst TaskInstance
				if json.NewDecoder(f).Decode(&inst) == nil && inst.Status == TaskStatusRunning {
					endTime := time.Now().UTC()
					os.MkdirAll(filepath.Join(runDir, e.Name()), 0755)
					writeTaskMeta(runDir, e.Name(), ps.CurrentRunID, ps.PipelineID, TaskStatusCrashed, inst.StartedAt, &endTime, -1)
					m.logger.Info("task status changed", "run_id", ps.CurrentRunID, "task", e.Name(), "status", TaskStatusCrashed, "reason", "stale_lock")
					m.appendEvent(ps.CurrentRunID, "%s task=%s status=%s reason=stale_lock", time.Now().UTC().Format(time.RFC3339), e.Name(), TaskStatusCrashed)
				}
				f.Close()
			}
		}
		m.pipelineStatus.SetStatus(ps.PipelineID, "idle")
	}

	m.clearState()
	return nil
}

func pidAlive(pid int) bool {
	process, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	err = process.Signal(syscall.Signal(0))
	return err == nil
}
