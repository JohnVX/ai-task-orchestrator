package pipeline

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Status values for a pipeline.
const (
	StatusIdle    = "idle"
	StatusRunning = "running"
)

// TaskRef references a task in a pipeline with optional timeout overrides.
// When pointer fields are nil, the task's default timeout settings are inherited.
type TaskRef struct {
	Name           string  `json:"name"`
	TimeoutSeconds *int    `json:"timeout_seconds,omitempty"` // nil=inherit, 0=disable, >0=seconds
	OnTimeout      *string `json:"on_timeout,omitempty"`      // nil=inherit
}

// Pipeline represents a named, ordered sequence of tasks.
type Pipeline struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	Tasks     []TaskRef `json:"tasks"`
	CreatedAt time.Time `json:"created_at"`
	Status    string    `json:"status"`
	Schedule  string    `json:"schedule,omitempty"`
}

// TaskChecker is the interface pipeline needs from the task package.
type TaskChecker interface {
	Exists(name string) bool
	Pipelines(name string) ([]string, error)
}

// RunCleaner is the interface pipeline needs from the runner package.
type RunCleaner interface {
	DeleteRuns(pipelineID string) error
	IsRunning(pipelineID string) bool
}

// Manager handles pipeline CRUD and task ordering.
type Manager struct {
	pipelinesDir string
	taskMgr      TaskChecker
	runCleaner   RunCleaner
}

// NewManager creates a Manager. It ensures the pipelines directory exists.
func NewManager(pipelinesDir string, taskMgr TaskChecker, runCleaner RunCleaner) *Manager {
	os.MkdirAll(pipelinesDir, 0755)
	return &Manager{pipelinesDir: pipelinesDir, taskMgr: taskMgr, runCleaner: runCleaner}
}

// --- helpers ---

func (m *Manager) pipelinePath(id string) string {
	return filepath.Join(m.pipelinesDir, id+".json")
}

func (m *Manager) readPipeline(id string) (*Pipeline, error) {
	f, err := os.Open(m.pipelinePath(id))
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var p Pipeline
	if err := json.NewDecoder(f).Decode(&p); err != nil {
		return nil, fmt.Errorf("parse pipeline %s: %w", id, err)
	}
	return &p, nil
}

func (m *Manager) writePipeline(p *Pipeline) error {
	f, err := os.Create(m.pipelinePath(p.ID))
	if err != nil {
		return err
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	return enc.Encode(p)
}

// nextID generates the next pipeline ID by scanning existing files.
func (m *Manager) nextID() string {
	entries, err := os.ReadDir(m.pipelinesDir)
	if err != nil {
		return "pipeline-1"
	}
	maxN := 0
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		rest := strings.TrimSuffix(strings.TrimPrefix(e.Name(), "pipeline-"), ".json")
		if n, err := strconv.Atoi(rest); err == nil && n > maxN {
			maxN = n
		}
	}
	return fmt.Sprintf("pipeline-%d", maxN+1)
}

// --- public methods ---

// Create writes a new pipeline definition.
func (m *Manager) Create(name string, schedule string) (*Pipeline, error) {
	all, err := m.All()
	if err != nil {
		return nil, err
	}
	for _, p := range all {
		if strings.EqualFold(p.Name, name) {
			return nil, fmt.Errorf("pipeline name %q already exists", name)
		}
	}

	p := &Pipeline{
		ID:        m.nextID(),
		Name:      name,
		Tasks:     []TaskRef{},
		CreatedAt: time.Now().UTC(),
		Status:    StatusIdle,
		Schedule:  schedule,
	}
	if err := m.writePipeline(p); err != nil {
		return nil, err
	}
	return p, nil
}

// Delete removes a pipeline and its associated run data.
func (m *Manager) Delete(id string) error {
	p, err := m.readPipeline(id)
	if err != nil {
		return err
	}
	if p.Status == StatusRunning {
		return fmt.Errorf("pipeline %s is running, stop it first", id)
	}
	if m.runCleaner != nil && m.runCleaner.IsRunning(id) {
		return fmt.Errorf("pipeline %s is running, stop it first", id)
	}
	if m.runCleaner != nil {
		if err := m.runCleaner.DeleteRuns(id); err != nil {
			return fmt.Errorf("delete runs for pipeline %s: %w", id, err)
		}
		// Re-check: Start() may have registered between the first check and now.
		if m.runCleaner.IsRunning(id) {
			return fmt.Errorf("pipeline %s is running, stop it first", id)
		}
	}
	return os.Remove(m.pipelinePath(id))
}

// Get returns a pipeline by ID.
func (m *Manager) Get(id string) (*Pipeline, error) {
	return m.readPipeline(id)
}

// All returns all pipelines, sorted by creation time.
func (m *Manager) All() ([]Pipeline, error) {
	entries, err := os.ReadDir(m.pipelinesDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var pipes []Pipeline
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		id := strings.TrimSuffix(e.Name(), ".json")
		p, err := m.readPipeline(id)
		if err != nil {
			continue
		}
		pipes = append(pipes, *p)
	}
	sort.Slice(pipes, func(i, j int) bool { return pipes[i].CreatedAt.Before(pipes[j].CreatedAt) })
	return pipes, nil
}

// AddTask appends a task to the pipeline's task list. The task must exist.
func (m *Manager) AddTask(pipelineID, taskName string) error {
	p, err := m.readPipeline(pipelineID)
	if err != nil {
		return err
	}
	if !m.taskMgr.Exists(taskName) {
		return fmt.Errorf("task %q does not exist", taskName)
	}
	p.Tasks = append(p.Tasks, TaskRef{Name: taskName})
	return m.writePipeline(p)
}

// RemoveTask removes a task from the pipeline's task list.
func (m *Manager) RemoveTask(pipelineID, taskName string) error {
	p, err := m.readPipeline(pipelineID)
	if err != nil {
		return err
	}
	for i, t := range p.Tasks {
		if t.Name == taskName {
			p.Tasks = append(p.Tasks[:i], p.Tasks[i+1:]...)
			return m.writePipeline(p)
		}
	}
	return nil
}

// ReorderTasks sets the task list to a new order, preserving existing configs.
func (m *Manager) ReorderTasks(pipelineID string, taskNames []string) error {
	p, err := m.readPipeline(pipelineID)
	if err != nil {
		return err
	}
	nameToRef := make(map[string]TaskRef, len(p.Tasks))
	for _, t := range p.Tasks {
		nameToRef[t.Name] = t
	}
	newTasks := make([]TaskRef, 0, len(taskNames))
	for _, name := range taskNames {
		if ref, ok := nameToRef[name]; ok {
			newTasks = append(newTasks, ref)
		} else {
			newTasks = append(newTasks, TaskRef{Name: name})
		}
	}
	p.Tasks = newTasks
	return m.writePipeline(p)
}

// SetStatus updates the pipeline status.
func (m *Manager) SetStatus(id, status string) error {
	p, err := m.readPipeline(id)
	if err != nil {
		return err
	}
	p.Status = status
	return m.writePipeline(p)
}

// SetSchedule updates the cron schedule for a pipeline. Empty string disables scheduling.
func (m *Manager) SetSchedule(id, schedule string) error {
	p, err := m.readPipeline(id)
	if err != nil {
		return err
	}
	if p.Status == StatusRunning {
		return fmt.Errorf("cannot modify schedule while pipeline is running")
	}
	p.Schedule = schedule
	return m.writePipeline(p)
}

// SetTaskConfig updates timeout overrides for a specific task within a pipeline.
// Pass nil for timeoutSeconds / onTimeout to inherit the task default.
// onTimeout, when non-nil, must be "skip" or "fail".
func (m *Manager) SetTaskConfig(pipelineID, taskName string, timeoutSeconds *int, onTimeout *string) error {
	if onTimeout != nil && *onTimeout != "skip" && *onTimeout != "fail" {
		return fmt.Errorf("on_timeout must be \"skip\", \"fail\", or null to inherit")
	}
	p, err := m.readPipeline(pipelineID)
	if err != nil {
		return err
	}
	if p.Status == StatusRunning {
		return fmt.Errorf("cannot modify task config while pipeline is running")
	}
	for i, t := range p.Tasks {
		if t.Name == taskName {
			p.Tasks[i].TimeoutSeconds = timeoutSeconds
			p.Tasks[i].OnTimeout = onTimeout
			return m.writePipeline(p)
		}
	}
	return fmt.Errorf("task %q not found in pipeline %s", taskName, pipelineID)
}

// IsRunning returns true if the pipeline is currently running.
func (m *Manager) IsRunning(id string) bool {
	p, err := m.Get(id)
	if err != nil {
		return false
	}
	return p.Status == StatusRunning
}
