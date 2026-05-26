package api

import (
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/ai-task-orchestrator/internal/pipeline"
	"github.com/ai-task-orchestrator/internal/runner"
	"github.com/ai-task-orchestrator/internal/task"
)

// Handler holds references to the domain managers and serves HTTP endpoints.
type Handler struct {
	Task     *task.Manager
	Pipeline *pipeline.Manager
	Runner   *runner.Manager
	dataDir  string
	tmpl     *template.Template
	staticFS http.FileSystem
	logger   *slog.Logger
}

// NewHandler creates a Handler.
func NewHandler(tm *task.Manager, pm *pipeline.Manager, rm *runner.Manager, dataDir string, tmpl *template.Template, staticFS http.FileSystem, logger *slog.Logger) *Handler {
	return &Handler{Task: tm, Pipeline: pm, Runner: rm, dataDir: dataDir, tmpl: tmpl, staticFS: staticFS, logger: logger}
}

// Router returns an http.Handler that serves all routes.
func (h *Handler) Router() http.Handler {
	mux := http.NewServeMux()

	// Task routes
	mux.HandleFunc("GET /api/tasks", h.handleListTasks)
	mux.HandleFunc("POST /api/tasks", h.handleUploadTask)
	mux.HandleFunc("GET /api/tasks/{name}", h.handleGetTask)
	mux.HandleFunc("PUT /api/tasks/{name}", h.handleUpdateTask)
	mux.HandleFunc("DELETE /api/tasks/{name}", h.handleDeleteTask)
	mux.HandleFunc("GET /api/tasks/{name}/download", h.handleDownloadTask)

	// Pipeline routes
	mux.HandleFunc("GET /api/pipelines", h.handleListPipelines)
	mux.HandleFunc("POST /api/pipelines", h.handleCreatePipeline)
	mux.HandleFunc("GET /api/pipelines/{id}", h.handleGetPipeline)
	mux.HandleFunc("PUT /api/pipelines/{id}", h.handleUpdatePipeline)
	mux.HandleFunc("DELETE /api/pipelines/{id}", h.handleDeletePipeline)
	mux.HandleFunc("POST /api/pipelines/{id}/start", h.handleStartPipeline)
	mux.HandleFunc("POST /api/pipelines/{id}/stop", h.handleStopPipeline)

	// Run routes
	mux.HandleFunc("GET /api/runs", h.handleListRuns)
	mux.HandleFunc("GET /api/runs/{id}", h.handleGetRun)
	mux.HandleFunc("GET /api/runs/{id}/events", h.handleGetRunEvents)
	mux.HandleFunc("DELETE /api/runs/{id}", h.handleDeleteRun)
	mux.HandleFunc("POST /api/runs/{id}/continue", h.handleContinueRun)

	// State
	mux.HandleFunc("GET /api/state", h.handleState)

	// Static files
	fs := http.FileServer(h.staticFS)
	mux.Handle("GET /static/", http.StripPrefix("/static/", fs))

	// Page
	mux.HandleFunc("GET /", h.handleIndex)

	return mux
}

// --- helpers ---

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func (h *Handler) writeError(w http.ResponseWriter, status int, msg string) {
	if status >= 500 {
		h.logger.Error("request error", "status", status, "error", msg)
	}
	writeJSON(w, status, map[string]string{"error": msg})
}

// --- task handlers ---

func (h *Handler) handleListTasks(w http.ResponseWriter, r *http.Request) {
	tasks, err := h.Task.All()
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if tasks == nil {
		tasks = []task.Meta{}
	}
	writeJSON(w, http.StatusOK, tasks)
}

func (h *Handler) handleUploadTask(w http.ResponseWriter, r *http.Request) {
	r.ParseMultipartForm(50 << 20) // 50 MB max
	file, header, err := r.FormFile("file")
	if err != nil {
		h.writeError(w, http.StatusBadRequest, "missing file field")
		return
	}
	defer file.Close()

	if !strings.HasSuffix(header.Filename, ".tar") {
		h.writeError(w, http.StatusBadRequest, "file must be a .tar archive")
		return
	}

	tmpDir, err := os.MkdirTemp("", "task-upload-")
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	defer os.RemoveAll(tmpDir)

	tmpPath := filepath.Join(tmpDir, header.Filename)
	out, err := os.Create(tmpPath)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if _, err := io.Copy(out, file); err != nil {
		out.Close()
		h.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	out.Close()

	meta, err := h.Task.Upload(tmpPath)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	h.logger.Info("task uploaded", "name", meta.Name)
	writeJSON(w, http.StatusCreated, meta)
}

func (h *Handler) handleGetTask(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	meta, err := h.Task.Get(name)
	if err != nil {
		h.writeError(w, http.StatusNotFound, "task not found")
		return
	}
	readme, _ := h.Task.ParseReadme(name)
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"meta":   meta,
		"readme": readme,
	})
}

func (h *Handler) handleUpdateTask(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	var body struct {
		RunCommand        string `json:"run_command"`
		StopCommand       string `json:"stop_command"`
		TimeoutEnabled    bool   `json:"timeout_enabled"`
		TimeoutSeconds    int    `json:"timeout_seconds"`
		OnTimeout         string `json:"on_timeout"`
		ContinueOnFailure bool   `json:"continue_on_failure"`
		RetryCount        int    `json:"retry_count"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		h.writeError(w, http.StatusBadRequest, "invalid JSON")
		return
	}
	if err := h.Task.SetConfig(name, body.RunCommand, body.StopCommand, body.TimeoutEnabled, body.TimeoutSeconds, body.OnTimeout, body.ContinueOnFailure, body.RetryCount); err != nil {
		h.writeError(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handler) handleDeleteTask(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if err := h.Task.Delete(name); err != nil {
		msg := err.Error()
		if strings.Contains(msg, "not found") {
			h.writeError(w, http.StatusNotFound, msg)
		} else {
			h.writeError(w, http.StatusConflict, msg)
		}
		return
	}
	h.logger.Info("task deleted", "name", name)
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handler) handleDownloadTask(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	tarPath, err := h.Task.Export(name)
	if err != nil {
		msg := err.Error()
		if strings.Contains(msg, "not found") {
			h.writeError(w, http.StatusNotFound, "task not found")
		} else {
			h.writeError(w, http.StatusInternalServerError, msg)
		}
		return
	}
	defer os.Remove(tarPath)

	w.Header().Set("Content-Type", "application/x-tar")
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s.tar"`, name))
	http.ServeFile(w, r, tarPath)
}

// --- pipeline handlers ---

func (h *Handler) handleListPipelines(w http.ResponseWriter, r *http.Request) {
	pipelines, err := h.Pipeline.All()
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if pipelines == nil {
		pipelines = []pipeline.Pipeline{}
	}
	writeJSON(w, http.StatusOK, pipelines)
}

func (h *Handler) handleCreatePipeline(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Name       string `json:"name"`
		Schedule   string `json:"schedule"`
		WebhookURL string `json:"webhook_url"`
		LoopCount         *int    `json:"loop_count,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Name == "" {
		h.writeError(w, http.StatusBadRequest, "pipeline name required")
		return
	}
	if body.Schedule != "" && !runner.ValidCron(body.Schedule) {
		h.writeError(w, http.StatusBadRequest, "invalid cron expression")
		return
	}
	p, err := h.Pipeline.Create(body.Name, body.Schedule, body.WebhookURL, body.LoopCount)
	if err != nil {
		h.writeError(w, http.StatusConflict, err.Error())
		return
	}
	h.logger.Info("pipeline created", "id", p.ID, "name", p.Name)
	writeJSON(w, http.StatusCreated, p)
}

func (h *Handler) handleGetPipeline(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	p, err := h.Pipeline.Get(id)
	if err != nil {
		h.writeError(w, http.StatusNotFound, "pipeline not found")
		return
	}

	// Enrich tasks with metadata and pipeline-level overrides.
	type taskInfo struct {
		Name              string  `json:"name"`
		RunCmd            string  `json:"run_command"`
		StopCmd           string  `json:"stop_command"`
		Readme            string  `json:"readme"`
		TimeoutSeconds    *int    `json:"timeout_seconds,omitempty"`
		OnTimeout         *string `json:"on_timeout,omitempty"`
		ContinueOnFailure *bool   `json:"continue_on_failure,omitempty"`
		RetryCount        *int    `json:"retry_count,omitempty"`
	}
	tasks := make([]taskInfo, 0, len(p.Tasks))
	for _, ref := range p.Tasks {
		info := taskInfo{
			Name:              ref.Name,
			TimeoutSeconds:    ref.TimeoutSeconds,
			OnTimeout:         ref.OnTimeout,
			ContinueOnFailure: ref.ContinueOnFailure,
			RetryCount:        ref.RetryCount,
		}
		if meta, err := h.Task.Get(ref.Name); err == nil {
			info.RunCmd = meta.RunCommand
			info.StopCmd = meta.StopCommand
		}
		if readme, found := h.Task.ParseReadme(ref.Name); found {
			info.Readme = readme
		}
		tasks = append(tasks, info)
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"pipeline": p,
		"tasks":    tasks,
	})
}

func (h *Handler) handleUpdatePipeline(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var body struct {
		Action            string   `json:"action"`
		TaskName          string   `json:"task_name"`
		TaskIndex         int      `json:"task_index"`
		TaskIndices       []int    `json:"task_indices"`
		Tasks             []string `json:"tasks"`
		Schedule          string   `json:"schedule"`
		WebhookURL        string   `json:"webhook_url"`
		LoopCount         *int    `json:"loop_count,omitempty"`
		TimeoutSeconds    *int     `json:"timeout_seconds,omitempty"`
		OnTimeout         *string  `json:"on_timeout,omitempty"`
		ContinueOnFailure *bool    `json:"continue_on_failure,omitempty"`
		RetryCount        *int     `json:"retry_count,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		h.writeError(w, http.StatusBadRequest, "invalid JSON")
		return
	}

	var err error
	switch body.Action {
	case "add_task":
		err = h.Pipeline.AddTask(id, body.TaskName)
	case "remove_task":
		err = h.Pipeline.RemoveTask(id, body.TaskIndex)
	case "reorder":
		err = h.Pipeline.ReorderTasks(id, body.TaskIndices)
	case "set_schedule":
		if body.Schedule != "" && !runner.ValidCron(body.Schedule) {
			h.writeError(w, http.StatusBadRequest, "invalid cron expression")
			return
		}
		err = h.Pipeline.SetSchedule(id, body.Schedule)
	case "set_webhook":
		err = h.Pipeline.SetWebhook(id, body.WebhookURL)
		case "set_loop":
			err = h.Pipeline.SetLoopCount(id, body.LoopCount)
	case "set_task_config":
		err = h.Pipeline.SetTaskConfig(id, body.TaskIndex, body.TimeoutSeconds, body.OnTimeout, body.ContinueOnFailure, body.RetryCount)
	default:
		h.writeError(w, http.StatusBadRequest, "unknown action: "+body.Action)
		return
	}

	if err != nil {
		h.writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handler) handleDeletePipeline(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := h.Pipeline.Delete(id); err != nil {
		msg := err.Error()
		if strings.Contains(msg, "not found") || strings.Contains(msg, "no such file") {
			h.writeError(w, http.StatusNotFound, "pipeline not found")
		} else {
			h.writeError(w, http.StatusConflict, msg)
		}
		return
	}
	h.logger.Info("pipeline deleted", "id", id)
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handler) handleStartPipeline(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	p, err := h.Pipeline.Get(id)
	if err != nil {
		h.writeError(w, http.StatusNotFound, "pipeline not found")
		return
	}
	if len(p.Tasks) == 0 {
		h.writeError(w, http.StatusBadRequest, "pipeline has no tasks")
		return
	}
	runTasks := make([]runner.RunTask, len(p.Tasks))
	for i, ref := range p.Tasks {
		runTasks[i] = runner.RunTask{
			Name:              ref.Name,
			TimeoutSeconds:    ref.TimeoutSeconds,
			OnTimeout:         ref.OnTimeout,
			ContinueOnFailure: ref.ContinueOnFailure,
			RetryCount:        ref.RetryCount,
		}
	}
	runID, err := h.Runner.Start(id, runTasks, p.WebhookURL, p.Name, resolveLoopCount(p.LoopCount))
	if err != nil {
		h.writeError(w, http.StatusConflict, err.Error())
		return
	}
	h.logger.Info("pipeline started via API", "pipeline_id", id, "run_id", runID)
	writeJSON(w, http.StatusOK, map[string]string{"run_id": runID})
}

func (h *Handler) handleStopPipeline(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	state, _ := h.Runner.State()
	running := false
	if state != nil {
		for _, rp := range state.RunningPipelines {
			if rp.PipelineID == id {
				running = true
				break
			}
		}
	}
	if !running {
		h.writeError(w, http.StatusBadRequest, "pipeline is not running")
		return
	}
	if err := h.Runner.Stop(id); err != nil {
		h.writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	h.logger.Info("pipeline stop requested", "pipeline_id", id)
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}
func (h *Handler) handleContinueRun(w http.ResponseWriter, r *http.Request) {
	runID := r.PathValue("id")
	var body struct {
		PipelineID string `json:"pipeline_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.PipelineID == "" {
		h.writeError(w, http.StatusBadRequest, "pipeline_id required")
		return
	}
	p, err := h.Pipeline.Get(body.PipelineID)
	if err != nil {
		h.writeError(w, http.StatusNotFound, "pipeline not found")
		return
	}
	runTasks := make([]runner.RunTask, len(p.Tasks))
	for i, ref := range p.Tasks {
		runTasks[i] = runner.RunTask{
			Name:              ref.Name,
			TimeoutSeconds:    ref.TimeoutSeconds,
			OnTimeout:         ref.OnTimeout,
			ContinueOnFailure: ref.ContinueOnFailure,
			RetryCount:        ref.RetryCount,
		}
	}
	if err := h.Runner.ContinueRun(body.PipelineID, runID, runTasks, p.WebhookURL, p.Name); err != nil {
		h.writeError(w, http.StatusConflict, err.Error())
		return
	}
	h.logger.Info("run continued", "run_id", runID, "pipeline_id", body.PipelineID)
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// --- run handlers ---

func (h *Handler) handleListRuns(w http.ResponseWriter, r *http.Request) {
	entries, err := os.ReadDir(filepath.Join(h.dataDir, "runs"))
	if err != nil {
		writeJSON(w, http.StatusOK, []interface{}{})
		return
	}

	type runSummary struct {
		RunID      string `json:"run_id"`
		PipelineID string `json:"pipeline_id"`
		TaskCount  int    `json:"task_count"`
		Size       int64  `json:"size"`
		Status     string `json:"status"`
	}
	var runs []runSummary
	filterPipeline := r.URL.Query().Get("pipeline_id")

	for _, e := range entries {
		if !e.IsDir() || !strings.HasPrefix(e.Name(), "run-") {
			continue
		}
		instances, _ := h.Runner.RunInfo(e.Name())
		if len(instances) == 0 {
			continue
		}
		pipelineID := instances[0].PipelineID
		if filterPipeline != "" && pipelineID != filterPipeline {
			continue
		}
		runStatus := runner.ComputeRunStatus(instances)
		size, _ := h.Runner.RunDirSize(e.Name())
		runs = append(runs, runSummary{
			RunID:      e.Name(),
			PipelineID: pipelineID,
			TaskCount:  len(instances),
			Size:       size,
			Status:     runStatus,
		})
	}
	if runs == nil {
		runs = []runSummary{}
	}
	sort.Slice(runs, func(i, j int) bool {
		si := strings.LastIndex(runs[i].RunID, "-")
		sj := strings.LastIndex(runs[j].RunID, "-")
		ni, _ := strconv.Atoi(runs[i].RunID[si+1:])
		nj, _ := strconv.Atoi(runs[j].RunID[sj+1:])
		return ni > nj
	})
	writeJSON(w, http.StatusOK, runs)
}

func (h *Handler) handleGetRun(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	if r.URL.Query().Get("log") == "1" {
		taskName := r.URL.Query().Get("task")
		if taskName == "" {
			h.writeError(w, http.StatusBadRequest, "task parameter required for logs")
			return
		}
		taskIdxStr := r.URL.Query().Get("task_idx")
		taskIdx, err := strconv.Atoi(taskIdxStr)
		if taskIdxStr != "" && err != nil {
			h.writeError(w, http.StatusBadRequest, "invalid task_idx parameter")
			return
		}
		stdout, stderr, err := h.Runner.RunLog(id, taskName, taskIdx)
		if err != nil {
			h.writeError(w, http.StatusNotFound, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{
			"stdout": stdout,
			"stderr": stderr,
		})
		return
	}

	instances, err := h.Runner.RunInfo(id)
	if err != nil {
		h.writeError(w, http.StatusNotFound, "run not found")
		return
	}
	if instances == nil {
		instances = []runner.TaskInstance{}
	}
	writeJSON(w, http.StatusOK, instances)
}

func (h *Handler) handleGetRunEvents(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	content, err := h.Runner.RunEvents(id)
	if err != nil {
		h.writeError(w, http.StatusNotFound, err.Error())
		return
	}
	if content == "" {
		content = "(no events)\n"
	}
	writeJSON(w, http.StatusOK, map[string]string{"events": content})
}

func (h *Handler) handleDeleteRun(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	err := h.Runner.DeleteRun(id)
	if err == nil {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
		return
	}
	msg := err.Error()
	switch {
	case strings.Contains(msg, "not found"):
		h.writeError(w, http.StatusNotFound, msg)
	case strings.Contains(msg, "active run"):
		h.writeError(w, http.StatusConflict, msg)
	case strings.Contains(msg, "invalid"):
		h.writeError(w, http.StatusBadRequest, msg)
	default:
		h.writeError(w, http.StatusInternalServerError, msg)
	}
}

// --- state ---

func (h *Handler) handleState(w http.ResponseWriter, r *http.Request) {
	state, err := h.Runner.State()
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if state == nil {
		state = &runner.OrchestratorState{}
	}
	writeJSON(w, http.StatusOK, state)
}

// --- page ---

func (h *Handler) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}

	tasks, _ := h.Task.All()
	pipelines, _ := h.Pipeline.All()
	state, _ := h.Runner.State()

	if tasks == nil {
		tasks = []task.Meta{}
	}
	if pipelines == nil {
		pipelines = []pipeline.Pipeline{}
	}
	if state == nil {
		state = &runner.OrchestratorState{}
	}

	data := map[string]interface{}{
		"Tasks":     tasks,
		"Pipelines": pipelines,
		"State":     state,
	}

	h.tmpl.Execute(w, data)
}

// RecoverOnStartup is called by main to clean up stale locks.
func (h *Handler) RecoverOnStartup() error {
	return h.Runner.RecoverOnStartup()
}


func resolveLoopCount(lc *int) int {
	if lc == nil {
		return 1
	}
	return *lc
}
