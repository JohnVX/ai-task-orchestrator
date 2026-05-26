package runner

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
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
	TaskStatusTimeout = "timeout"
)

// RunTask describes a task to execute within a pipeline run, with optional
// overrides. Pointer fields are nil when the task default should be inherited.
type RunTask struct {
	Name              string
	TimeoutSeconds    *int    // nil=inherit 0=disable >0=override seconds
	OnTimeout         *string // nil=inherit "skip" or "fail"
	ContinueOnFailure *bool   // nil=inherit
	RetryCount        *int    // nil=inherit, 0=no retry, >0=max retries on timeout
}

// TaskInstance records the result of a single task execution within a run.
type TaskInstance struct {
	TaskName   string     `json:"task_name"`
	RunID      string     `json:"run_id"`
	PipelineID string     `json:"pipeline_id"`
	Status     string     `json:"status"`
	StartedAt  *time.Time `json:"started_at"`
	EndedAt    *time.Time `json:"ended_at"`
	ExitCode   int        `json:"exit_code"`
	Index      int        `json:"index"`
}

// PipelineRunState tracks a single running pipeline within the state file.
type PipelineRunState struct {
	PipelineID   string `json:"pipeline_id"`
	CurrentTask  string `json:"current_task"`
	CurrentRunID string `json:"current_run_id"`
	TaskIndex    int    `json:"task_index"`
	Iteration    int    `json:"iteration"`
	LoopTotal    int    `json:"loop_total"`
}

// OrchestratorState is the global state persisted to orchestrator_state.json.
// When PID is non-zero it acts as a single-instance lock. RunningPipelines
// tracks all currently executing pipelines for crash recovery. StartTime is
// the process start time in clock ticks since boot, used to detect PID reuse.
type OrchestratorState struct {
	PID              int                `json:"pid"`
	StartTime        uint64             `json:"start_time"`
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
	stateMu sync.Mutex
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

func (m *Manager) nextRunID(pipelineID string) string {
	entries, _ := os.ReadDir(m.runsDir)
	prefix := "run-" + pipelineID + "-"
	maxN := 0
	for _, e := range entries {
		if !strings.HasPrefix(e.Name(), prefix) {
			continue
		}
		rest := strings.TrimPrefix(e.Name(), prefix)
		if n, err := strconv.Atoi(rest); err == nil && n > maxN {
			maxN = n
		}
	}
	return fmt.Sprintf("%s%06d", prefix, maxN+1)
}

// runSeq extracts the numeric suffix from a run ID (e.g., "run-p1-000001" → 1).
func runSeq(runID string) int {
	idx := strings.LastIndex(runID, "-")
	if idx < 0 {
		return 0
	}
	n, _ := strconv.Atoi(runID[idx+1:])
	return n
}

func clearDir(path string) error {
	if err := os.RemoveAll(path); err != nil {
		return err
	}
	return os.MkdirAll(path, 0755)
}

func writeTaskMeta(taskDir, taskName, runID, pipelineID, status string, startedAt, endedAt *time.Time, exitCode, index int) error {
	inst := TaskInstance{
		TaskName:   taskName,
		RunID:      runID,
		PipelineID: pipelineID,
		Status:     status,
		StartedAt:  startedAt,
		EndedAt:    endedAt,
		ExitCode:   exitCode,
		Index:      index,
	}
	f, err := os.Create(filepath.Join(taskDir, "meta.json"))
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
// loopCount: 0=forever, >0=exact count. Passing 0 with no loop config = run once (handled by caller).
func (m *Manager) Start(pipelineID string, tasks []RunTask, webhookURL string, pipelineName string, loopCount int) (runID string, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.running[pipelineID]; exists {
		return "", fmt.Errorf("pipeline %q is already running", pipelineID)
	}
	if len(tasks) == 0 {
		return "", fmt.Errorf("pipeline %q has no tasks", pipelineID)
	}

	runID = m.nextRunID(pipelineID)
	runDir := filepath.Join(m.runsDir, runID)

	os.MkdirAll(filepath.Join(runDir, "task-data-1"), 0755)
	os.MkdirAll(filepath.Join(runDir, "task-data-2"), 0755)
	for i, t := range tasks {
		os.MkdirAll(filepath.Join(runDir, fmt.Sprintf("%s-%d", t.Name, i)), 0755)
	}

	m.stateMu.Lock()
	state, _ := m.readState()
	if state.PID == 0 {
		state.PID = os.Getpid()
		state.StartTime = processStartTime(os.Getpid())
	}
	state.RunningPipelines = append(state.RunningPipelines, PipelineRunState{
			Iteration:    1,
			LoopTotal:    loopCount,
		PipelineID:   pipelineID,
		CurrentTask:  tasks[0].Name,
		CurrentRunID: runID,
	})
	if err := m.writeState(state); err != nil {
		m.stateMu.Unlock()
		return "", fmt.Errorf("write state: %w", err)
	}
	m.stateMu.Unlock()

	m.pipelineStatus.SetStatus(pipelineID, "running")
	m.logger.Info("pipeline started", "pipeline_id", pipelineID, "run_id", runID)
	m.appendEvent(runID, "%s pipeline=%s event=pipeline_started", time.Now().UTC().Format(time.RFC3339), pipelineID)

	if loopCount == 0 {
		m.logger.Info("pipeline loop forever", "pipeline_id", pipelineID)
	} else if loopCount > 1 {
		m.logger.Info("pipeline loop configured", "pipeline_id", pipelineID, "loop_count", loopCount)
	}

	ctl := &runControl{stopCh: make(chan struct{})}
	m.running[pipelineID] = ctl

	go m.runLoop(pipelineID, runID, runDir, tasks, ctl, webhookURL, pipelineName, 0, loopCount, 0, loopCount)

	return runID, nil
}

// ContinueRun retries from the first non-successful task, reusing the same run directory.
// If the pipeline was part of a loop, remaining iterations are preserved.
func (m *Manager) ContinueRun(pipelineID, runID string, tasks []RunTask, webhookURL string, pipelineName string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.running[pipelineID]; exists {
		return fmt.Errorf("pipeline %q is already running", pipelineID)
	}
	if len(tasks) == 0 {
		return fmt.Errorf("pipeline %q has no tasks", pipelineID)
	}

	oldInstances, err := m.RunInfo(runID)
	if err != nil {
		return fmt.Errorf("cannot read run %q: %w", runID, err)
	}
	if len(oldInstances) > 0 && oldInstances[0].PipelineID != pipelineID {
		return fmt.Errorf("run %q does not belong to pipeline %q", runID, pipelineID)
	}

	startIdx := 0
	found := false
	for _, inst := range oldInstances {
		if inst.Status != TaskStatusSuccess && inst.Status != TaskStatusPending {
			if !found || inst.Index > startIdx {
				startIdx = inst.Index
				found = true
			}
		}
	}
	if !found {
		return fmt.Errorf("all tasks already succeeded")
	}

	runDir := filepath.Join(m.runsDir, runID)
	os.MkdirAll(filepath.Join(runDir, "task-data-1"), 0755)
	os.MkdirAll(filepath.Join(runDir, "task-data-2"), 0755)
	for i, t := range tasks {
		os.MkdirAll(filepath.Join(runDir, fmt.Sprintf("%s-%d", t.Name, i)), 0755)
	}

	remainingLoop, stoppedIteration, originalTotal := resolveRemainingLoop(runDir)

	m.stateMu.Lock()
	state, _ := m.readState()
	if state.PID == 0 {
		state.PID = os.Getpid()
		state.StartTime = processStartTime(os.Getpid())
	}
	state.RunningPipelines = append(state.RunningPipelines, PipelineRunState{
			Iteration:    stoppedIteration,
			LoopTotal:    originalTotal,
		PipelineID:   pipelineID,
		CurrentTask:  tasks[startIdx].Name,
		CurrentRunID: runID,
		TaskIndex:    startIdx,
	})
	if err := m.writeState(state); err != nil {
		m.stateMu.Unlock()
		return fmt.Errorf("write state: %w", err)
	}
	m.stateMu.Unlock()

	m.pipelineStatus.SetStatus(pipelineID, "running")
	m.logger.Info("pipeline continued", "pipeline_id", pipelineID, "run_id", runID, "from_index", startIdx)
	m.appendEvent(runID, "%s pipeline=%s event=continue_run from_index=%d", time.Now().UTC().Format(time.RFC3339), pipelineID, startIdx)

	ctl := &runControl{stopCh: make(chan struct{})}
	m.running[pipelineID] = ctl

	go m.runLoop(pipelineID, runID, runDir, tasks, ctl, webhookURL, pipelineName, startIdx, remainingLoop, stoppedIteration-1, originalTotal)

	return nil
}

func (m *Manager) runLoop(pipelineID, runID, runDir string, tasks []RunTask, ctl *runControl, webhookURL string, pipelineName string, startIdx int, loopCount int, iterationBase int, loopDisplay int) {
	defer func() {
		m.removeFromState(pipelineID)
		m.logger.Info("pipeline finished", "pipeline_id", pipelineID, "run_id", runID)
		m.appendEvent(runID, "%s pipeline=%s event=pipeline_finished", time.Now().UTC().Format(time.RFC3339), pipelineID)
		m.pipelineStatus.SetStatus(pipelineID, "idle")
		m.mu.Lock()
		delete(m.running, pipelineID)
		m.mu.Unlock()

		if webhookURL != "" {
			m.sendWebhook(webhookURL, pipelineID, runID, pipelineName)
		}
	}()

	for iteration := 0; loopCount == 0 || iteration < loopCount; iteration++ {
		if iteration > 0 {
			// Setup new run for this iteration.
			runID = m.nextRunID(pipelineID)
			runDir = filepath.Join(m.runsDir, runID)
			os.MkdirAll(filepath.Join(runDir, "task-data-1"), 0755)
			os.MkdirAll(filepath.Join(runDir, "task-data-2"), 0755)
			for i, t := range tasks {
				os.MkdirAll(filepath.Join(runDir, fmt.Sprintf("%s-%d", t.Name, i)), 0755)
			}

			m.updateStateRunID(pipelineID, runID, tasks[0].Name, iterationBase+iteration+1, loopDisplay)
			m.logger.Info("loop iteration started", "pipeline_id", pipelineID, "run_id", runID, "iteration", iterationBase+iteration+1)
			m.appendEvent(runID, "%s pipeline=%s event=pipeline_started iteration=%d", time.Now().UTC().Format(time.RFC3339), pipelineID, iteration+1)
			startIdx = 0
		}

		// Write iteration metadata for recovery (ContinueRun needs it).
		writeIterationMeta(runDir, iterationBase+iteration+1, loopDisplay)

		// Check stop before each iteration.
		select {
		case <-ctl.stopCh:
			return
		default:
		}

		taskLoop:
		for i, rt := range tasks {
			if i < startIdx {
				continue
			}
			taskName := rt.Name
			logName := fmt.Sprintf("%s[%d]", taskName, i+1)
			taskDir := filepath.Join(runDir, fmt.Sprintf("%s-%d", taskName, i))

			select {
			case <-ctl.stopCh:
				m.markTask(taskDir, taskName, runID, pipelineID, TaskStatusPending, nil, i)
				m.logger.Info("task status changed", "run_id", runID, "task", logName, "status", TaskStatusPending)
				m.appendEvent(runID, "%s task=%s status=%s", time.Now().UTC().Format(time.RFC3339), logName, TaskStatusPending)
				return
			default:
			}

			meta, err := m.taskMgr.Get(taskName)
			if err != nil {
				m.markTask(taskDir, taskName, runID, pipelineID, TaskStatusFailed, nil, i)
				m.logger.Info("task status changed", "run_id", runID, "task", logName, "status", TaskStatusFailed)
				m.appendEvent(runID, "%s task=%s status=%s", time.Now().UTC().Format(time.RFC3339), logName, TaskStatusFailed)
				return
			}

			m.updateState(pipelineID, taskName, runID, i)

			var writeBuf, readBuf string
			if i%2 == 0 {
				writeBuf, readBuf = "task-data-1", "task-data-2"
			} else {
				writeBuf, readBuf = "task-data-2", "task-data-1"
			}

			// Resolve effective configs (pipeline override > task default).
			timeoutSec := 0
			if rt.TimeoutSeconds != nil {
				timeoutSec = *rt.TimeoutSeconds
			} else if meta.TimeoutEnabled {
				timeoutSec = meta.TimeoutSeconds
			}

			onTimeout := "fail"
			if rt.OnTimeout != nil {
				onTimeout = *rt.OnTimeout
			} else if meta.OnTimeout != "" {
				onTimeout = meta.OnTimeout
			}

			continueOnFailure := false
			if rt.ContinueOnFailure != nil {
				continueOnFailure = *rt.ContinueOnFailure
			} else {
				continueOnFailure = meta.ContinueOnFailure
			}

			retryCount := 0
			if rt.RetryCount != nil {
				retryCount = *rt.RetryCount
			} else {
				retryCount = meta.RetryCount
			}
			maxAttempts := retryCount + 1

			var execErr error
			var timedOut bool
			var firstStartAt time.Time
			var finalAttempt int

			for attempt := 0; attempt < maxAttempts; attempt++ {
				finalAttempt = attempt
				if attempt > 0 {
					m.logger.Info("task retry", "run_id", runID, "task", logName, "attempt", attempt+1, "max_attempts", maxAttempts)
					m.appendEvent(runID, "%s task=%s event=retry attempt=%d/%d", time.Now().UTC().Format(time.RFC3339), logName, attempt+1, maxAttempts)
					if err := clearDir(filepath.Join(runDir, writeBuf)); err != nil {
						m.logger.Warn("clear write buffer", "dir", writeBuf, "error", err)
					}
				} else {
					if err := clearDir(filepath.Join(runDir, writeBuf)); err != nil {
						m.logger.Warn("clear write buffer", "dir", writeBuf, "error", err)
					}
				}

				attemptStart := time.Now().UTC()
				if attempt == 0 {
					firstStartAt = attemptStart
				}
				writeTaskMeta(taskDir, taskName, runID, pipelineID, TaskStatusRunning, &attemptStart, nil, -1, i)
				if attempt == 0 {
					m.logger.Info("task status changed", "run_id", runID, "task", logName, "status", TaskStatusRunning)
					m.appendEvent(runID, "%s task=%s status=%s", time.Now().UTC().Format(time.RFC3339), logName, TaskStatusRunning)
				}

				cmd := exec.Command("sh", "-c", meta.RunCommand)
				cmd.Dir = filepath.Join(m.dataDir, meta.PackagePath)
				cmd.Env = append(os.Environ(),
					"TASK_DATA_READ="+filepath.Join(runDir, readBuf),
					"TASK_DATA_WRITE="+filepath.Join(runDir, writeBuf),
					"TASK_DATA_1="+filepath.Join(runDir, "task-data-1"),
					"TASK_DATA_2="+filepath.Join(runDir, "task-data-2"),
				)

				stdoutF, cerr := os.Create(filepath.Join(taskDir, "stdout.log"))
				if cerr != nil {
					m.logger.Warn("create stdout log", "error", cerr)
				}
				stderrF, cerr := os.Create(filepath.Join(taskDir, "stderr.log"))
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
					execErr = err
					timedOut = false
					break
				}

				var timeoutCh <-chan time.Time
				if timeoutSec > 0 {
					timeoutCh = time.After(time.Duration(timeoutSec) * time.Second)
				}

				waitDone := make(chan error, 1)
				go func() { waitDone <- cmd.Wait() }()

				timedOut = false
				select {
				case execErr = <-waitDone:
				case <-ctl.stopCh:
					m.runStopCommand(meta)
					cmd.Process.Signal(syscall.SIGTERM)
					select {
					case execErr = <-waitDone:
					case <-time.After(10 * time.Second):
						cmd.Process.Kill()
						execErr = <-waitDone
					}
				case <-timeoutCh:
					timedOut = true
					m.logger.Info("task timeout", "run_id", runID, "task", logName, "timeout_seconds", timeoutSec, "attempt", attempt+1)
					m.appendEvent(runID, "%s task=%s event=timeout timeout=%ds attempt=%d", time.Now().UTC().Format(time.RFC3339), logName, timeoutSec, attempt+1)
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

				// Success: break out of retry loop.
				if execErr == nil && !timedOut {
					break
				}
				// Stop or non-timeout exit: don't retry.
				if !timedOut {
					break
				}
				// Timeout with retries remaining: loop again.
				if attempt+1 < maxAttempts {
					continue
				}
				break
			}

			endedAt := time.Now().UTC()

			if execErr != nil || timedOut {
				exitCode := -1
				if execErr != nil {
					if exitErr, ok := execErr.(*exec.ExitError); ok {
						exitCode = exitErr.ExitCode()
					}
				}
				status := TaskStatusFailed
				select {
				case <-ctl.stopCh:
					status = TaskStatusStopped
				default:
				}
				if timedOut && status != TaskStatusStopped {
					status = TaskStatusTimeout
				}
				writeTaskMeta(taskDir, taskName, runID, pipelineID, status, &firstStartAt, &endedAt, exitCode, i)
				m.logger.Info("task status changed", "run_id", runID, "task", logName, "status", status, "exit_code", exitCode)
				m.appendEvent(runID, "%s task=%s status=%s exit_code=%d", time.Now().UTC().Format(time.RFC3339), logName, status, exitCode)

				if timedOut && onTimeout == "skip" {
					m.logger.Info("task timed out but continuing pipeline", "run_id", runID, "task", logName)
					m.appendEvent(runID, "%s task=%s event=continuing_on_timeout", time.Now().UTC().Format(time.RFC3339), logName)
					continue
				}
				if continueOnFailure && status != TaskStatusStopped {
					m.logger.Info("task failed but continuing pipeline", "run_id", runID, "task", logName, "status", status)
					m.appendEvent(runID, "%s task=%s event=continuing_on_failure status=%s", time.Now().UTC().Format(time.RFC3339), logName, status)
					continue
				}
				if status == TaskStatusStopped {
					return
				}
				break taskLoop
			}

			writeTaskMeta(taskDir, taskName, runID, pipelineID, TaskStatusSuccess, &firstStartAt, &endedAt, 0, i)
			if finalAttempt > 0 {
				m.logger.Info("task status changed", "run_id", runID, "task", logName, "status", TaskStatusSuccess, "attempts", finalAttempt+1)
			} else {
				m.logger.Info("task status changed", "run_id", runID, "task", logName, "status", TaskStatusSuccess)
			}
			m.appendEvent(runID, "%s task=%s status=%s", time.Now().UTC().Format(time.RFC3339), logName, TaskStatusSuccess)

			clearDir(filepath.Join(runDir, readBuf))
		}
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

func (m *Manager) updateState(pipelineID, taskName, runID string, taskIndex int) {
	m.stateMu.Lock()
	defer m.stateMu.Unlock()
	state, _ := m.readState()
	for i, rp := range state.RunningPipelines {
		if rp.PipelineID == pipelineID {
			state.RunningPipelines[i].CurrentTask = taskName
			state.RunningPipelines[i].TaskIndex = taskIndex
			break
		}
	}
	m.writeState(state)
}

func (m *Manager) updateStateRunID(pipelineID, runID, taskName string, iteration, loopTotal int) {
	m.stateMu.Lock()
	defer m.stateMu.Unlock()
	state, _ := m.readState()
	for i, rp := range state.RunningPipelines {
		if rp.PipelineID == pipelineID {
			state.RunningPipelines[i].CurrentRunID = runID
			state.RunningPipelines[i].CurrentTask = taskName
			state.RunningPipelines[i].TaskIndex = 0
			state.RunningPipelines[i].Iteration = iteration
			state.RunningPipelines[i].LoopTotal = loopTotal
			break
		}
	}
	m.writeState(state)
}

func (m *Manager) markTask(taskDir, taskName, runID, pipelineID, status string, startedAt *time.Time, index int) {
	endedAt := time.Now().UTC()
	if startedAt == nil {
		startedAt = &endedAt
	}
	writeTaskMeta(taskDir, taskName, runID, pipelineID, status, startedAt, &endedAt, -1, index)
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

// StopAll terminates all running pipelines. Used during graceful shutdown.
func (m *Manager) StopAll() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for id, ctl := range m.running {
		select {
		case <-ctl.stopCh:
		default:
			close(ctl.stopCh)
		}
		m.logger.Info("pipeline stopping due to shutdown", "pipeline_id", id)
	}
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
	m.stateMu.Lock()
	defer m.stateMu.Unlock()
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
	sort.Slice(instances, func(i, j int) bool {
		return instances[i].Index < instances[j].Index
	})
	return instances, nil
}

// RunLog returns stdout/stderr content for a task instance within a run.
func (m *Manager) RunLog(runID, taskName string, taskIdx int) (stdout, stderr string, err error) {
	taskDir := filepath.Join(m.runsDir, runID, fmt.Sprintf("%s-%d", taskName, taskIdx))
	stdoutB, err1 := os.ReadFile(filepath.Join(taskDir, "stdout.log"))
	stderrB, err2 := os.ReadFile(filepath.Join(taskDir, "stderr.log"))
	if err1 != nil && err2 != nil {
		return "", "", fmt.Errorf("no logs found for %s-%d in %s", taskName, taskIdx, runID)
	}
	return string(stdoutB), string(stderrB), nil
}

// RunEvents returns the content of the per-run events log.
func (m *Manager) RunEvents(runID string) (string, error) {
	runDir := filepath.Join(m.runsDir, runID)
	if _, err := os.Stat(runDir); err != nil {
		if os.IsNotExist(err) {
			return "", fmt.Errorf("run %q not found", runID)
		}
		return "", err
	}
	data, err := os.ReadFile(filepath.Join(runDir, "events.log"))
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
	m.stateMu.Lock()
	defer m.stateMu.Unlock()
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

	m.stateMu.Lock()
	state, _ := m.readState()
	active := false
	for _, rp := range state.RunningPipelines {
		if rp.CurrentRunID == runID {
			active = true
			break
		}
	}
	m.stateMu.Unlock()
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
				if err := os.RemoveAll(runDir); err != nil {
					m.logger.Warn("delete runs: remove dir failed", "run_dir", runDir, "error", err)
				}
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

// webhookPayload is sent as JSON to the configured webhook URL upon pipeline completion.
type webhookPayload struct {
	Event        string `json:"event"`
	PipelineID   string `json:"pipeline_id"`
	PipelineName string `json:"pipeline_name"`
	RunID        string `json:"run_id"`
	Status       string `json:"status"`
	TaskCount    int    `json:"task_count"`
	StartedAt    string `json:"started_at,omitempty"`
	EndedAt      string `json:"ended_at,omitempty"`
	FailedTask   string `json:"failed_task,omitempty"`
}

func (m *Manager) sendWebhook(url, pipelineID, runID, pipelineName string) {
	runDir := filepath.Join(m.runsDir, runID)
	entries, err := os.ReadDir(runDir)
	if err != nil {
		m.logger.Warn("webhook: cannot read run dir", "run_id", runID, "error", err)
		return
	}

	var instances []TaskInstance
	for _, e := range entries {
		if !e.IsDir() || strings.HasPrefix(e.Name(), "task-data-") {
			continue
		}
		metaPath := filepath.Join(runDir, e.Name(), "meta.json")
		data, err := os.ReadFile(metaPath)
		if err != nil {
			continue
		}
		var inst TaskInstance
		if json.Unmarshal(data, &inst) != nil {
			continue
		}
		instances = append(instances, inst)
	}

	if len(instances) == 0 {
		return
	}

	// Don't notify for manually stopped pipelines — check any task has "stopped" status.
	for _, inst := range instances {
		if inst.Status == TaskStatusStopped {
			return
		}
	}

	sort.Slice(instances, func(i, j int) bool {
		return instances[i].Index < instances[j].Index
	})
	status := ComputeRunStatus(instances)

	startedAt := instances[0].StartedAt
	endedAt := instances[len(instances)-1].EndedAt

	failedTask := ""
	if status == "failed" {
		for _, inst := range instances {
			if inst.Status == TaskStatusFailed || inst.Status == TaskStatusCrashed ||
				inst.Status == TaskStatusStopped || inst.Status == TaskStatusTimeout {
				failedTask = inst.TaskName
				break
			}
		}
	}

	payload := webhookPayload{
		Event:        "pipeline_completed",
		PipelineID:   pipelineID,
		PipelineName: pipelineName,
		RunID:        runID,
		Status:       status,
		TaskCount:    len(instances),
		FailedTask:   failedTask,
	}
	if startedAt != nil {
		payload.StartedAt = startedAt.Format(time.RFC3339)
	}
	if endedAt != nil {
		payload.EndedAt = endedAt.Format(time.RFC3339)
	}

	body, err := json.Marshal(payload)
	if err != nil {
		m.logger.Warn("webhook: marshal error", "error", err)
		return
	}

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		m.logger.Warn("webhook: request failed", "url", url, "error", err)
		return
	}
	resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		m.logger.Warn("webhook: non-2xx response", "url", url, "status", resp.StatusCode)
		return
	}
	m.logger.Info("webhook sent", "pipeline_id", pipelineID, "run_id", runID, "status", status)
}

// ComputeRunStatus derives the overall run status from its task instances.
func ComputeRunStatus(instances []TaskInstance) string {
	if len(instances) == 0 {
		return "unknown"
	}
	isRunning := false
	hasHardFailure := false
	for _, inst := range instances {
		switch inst.Status {
		case TaskStatusRunning, TaskStatusPending:
			isRunning = true
		case TaskStatusFailed, TaskStatusCrashed, TaskStatusStopped:
			hasHardFailure = true
		}
	}
	if isRunning {
		return "running"
	}
	if instances[len(instances)-1].Status == TaskStatusSuccess {
		return "success"
	}
	if hasHardFailure {
		return "failed"
	}
	return "failed"
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
	if alive && state.StartTime > 0 {
		actualStart := processStartTime(state.PID)
		if actualStart != state.StartTime {
			alive = false // PID reused by a different process
		}
	}
	if alive {
		return fmt.Errorf("another orchestrator instance is running (PID %d)", state.PID)
	}

	for _, ps := range state.RunningPipelines {
		runDir := filepath.Join(m.runsDir, ps.CurrentRunID)
		taskDir := filepath.Join(runDir, fmt.Sprintf("%s-%d", ps.CurrentTask, ps.TaskIndex))
		os.MkdirAll(taskDir, 0755)
		now := time.Now().UTC()
		logCrash := fmt.Sprintf("%s[%d]", ps.CurrentTask, ps.TaskIndex+1)
		writeTaskMeta(taskDir, ps.CurrentTask, ps.CurrentRunID, ps.PipelineID, TaskStatusCrashed, nil, &now, -1, ps.TaskIndex)
		m.logger.Info("task status changed", "run_id", ps.CurrentRunID, "task", logCrash, "status", TaskStatusCrashed, "reason", "stale_lock")
		m.appendEvent(ps.CurrentRunID, "%s task=%s status=%s reason=stale_lock", time.Now().UTC().Format(time.RFC3339), logCrash, TaskStatusCrashed)

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
					writeTaskMeta(filepath.Join(runDir, e.Name()), inst.TaskName, ps.CurrentRunID, ps.PipelineID, TaskStatusCrashed, inst.StartedAt, &endTime, -1, inst.Index)
					m.logger.Info("task status changed", "run_id", ps.CurrentRunID, "task", fmt.Sprintf("%s[%d]", inst.TaskName, inst.Index+1), "status", TaskStatusCrashed, "reason", "stale_lock")
					m.appendEvent(ps.CurrentRunID, "%s task=%s status=%s reason=stale_lock", time.Now().UTC().Format(time.RFC3339), fmt.Sprintf("%s[%d]", inst.TaskName, inst.Index+1), TaskStatusCrashed)
				}
				f.Close()
			}
		}
		m.pipelineStatus.SetStatus(ps.PipelineID, "idle")
	}

	m.clearState()
	return nil
}

func writeIterationMeta(runDir string, iteration, loopTotal int) {
	f, err := os.Create(filepath.Join(runDir, "iteration.json"))
	if err != nil {
		return
	}
	defer f.Close()
	fmt.Fprintf(f, "{\"iteration\":%d,\"loop_total\":%d}\n", iteration, loopTotal)
}

func resolveRemainingLoop(runDir string) (remaining int, stoppedIteration int, originalTotal int) {
	data, err := os.ReadFile(filepath.Join(runDir, "iteration.json"))
	if err != nil {
		return 1, 1, 1
	}
	var it struct {
		Iteration int `json:"iteration"`
		LoopTotal int `json:"loop_total"`
	}
	if json.Unmarshal(data, &it) != nil {
		return 1, 1, 1
	}
	if it.LoopTotal <= 0 {
		return 0, it.Iteration, 0 // forever loop
	}
	remaining = it.LoopTotal - it.Iteration + 1
	if remaining < 1 {
		remaining = 1
	}
	return remaining, it.Iteration, it.LoopTotal
}

func pidAlive(pid int) bool {
	process, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	err = process.Signal(syscall.Signal(0))
	return err == nil
}

// processStartTime reads the process start time (field 22) from /proc/[pid]/stat.
// Returns the value in clock ticks since boot, or 0 on error.
func processStartTime(pid int) uint64 {
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return 0
	}
	s := string(data)
	// comm is the second field and may contain spaces and parens.
	// Find the last ')' to locate the end of the comm field.
	lastParen := strings.LastIndex(s, ")")
	if lastParen < 0 {
		return 0
	}
	fields := strings.Fields(s[lastParen+2:])
	if len(fields) < 20 {
		return 0
	}
	v, _ := strconv.ParseUint(fields[19], 10, 64)
	return v
}
