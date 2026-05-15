package api

import (
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"net/http"
	"os"
	"path/filepath"
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
}

// NewHandler creates a Handler.
func NewHandler(tm *task.Manager, pm *pipeline.Manager, rm *runner.Manager, dataDir string) *Handler {
	tmpl := template.Must(template.ParseFiles(filepath.Join("web", "templates", "index.html")))
	return &Handler{Task: tm, Pipeline: pm, Runner: rm, dataDir: dataDir, tmpl: tmpl}
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

	// State
	mux.HandleFunc("GET /api/state", h.handleState)

	// Static files
	fs := http.FileServer(http.Dir("web/static"))
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

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

// --- task handlers ---

func (h *Handler) handleListTasks(w http.ResponseWriter, r *http.Request) {
	tasks, err := h.Task.All()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
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
		writeError(w, http.StatusBadRequest, "missing file field")
		return
	}
	defer file.Close()

	if !strings.HasSuffix(header.Filename, ".tar") {
		writeError(w, http.StatusBadRequest, "file must be a .tar archive")
		return
	}

	tmpDir, err := os.MkdirTemp("", "task-upload-")
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	defer os.RemoveAll(tmpDir)

	tmpPath := filepath.Join(tmpDir, header.Filename)
	out, err := os.Create(tmpPath)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if _, err := io.Copy(out, file); err != nil {
		out.Close()
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	out.Close()

	meta, err := h.Task.Upload(tmpPath)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, meta)
}

func (h *Handler) handleGetTask(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	meta, err := h.Task.Get(name)
	if err != nil {
		writeError(w, http.StatusNotFound, "task not found")
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
		RunCommand  string `json:"run_command"`
		StopCommand string `json:"stop_command"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON")
		return
	}
	if err := h.Task.SetCommands(name, body.RunCommand, body.StopCommand); err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handler) handleDeleteTask(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if err := h.Task.Delete(name); err != nil {
		writeError(w, http.StatusConflict, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// --- pipeline handlers ---

func (h *Handler) handleListPipelines(w http.ResponseWriter, r *http.Request) {
	pipelines, err := h.Pipeline.All()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if pipelines == nil {
		pipelines = []pipeline.Pipeline{}
	}
	writeJSON(w, http.StatusOK, pipelines)
}

func (h *Handler) handleCreatePipeline(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Name string `json:"name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Name == "" {
		writeError(w, http.StatusBadRequest, "pipeline name required")
		return
	}
	p, err := h.Pipeline.Create(body.Name)
	if err != nil {
		writeError(w, http.StatusConflict, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, p)
}

func (h *Handler) handleGetPipeline(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	p, err := h.Pipeline.Get(id)
	if err != nil {
		writeError(w, http.StatusNotFound, "pipeline not found")
		return
	}

	// Enrich tasks with metadata.
	type taskInfo struct {
		Name     string `json:"name"`
		RunCmd   string `json:"run_command"`
		StopCmd  string `json:"stop_command"`
		Readme   string `json:"readme"`
	}
	tasks := make([]taskInfo, 0, len(p.Tasks))
	for _, tname := range p.Tasks {
		info := taskInfo{Name: tname}
		if meta, err := h.Task.Get(tname); err == nil {
			info.RunCmd = meta.RunCommand
			info.StopCmd = meta.StopCommand
		}
		if readme, found := h.Task.ParseReadme(tname); found {
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
		Action   string   `json:"action"`
		TaskName string   `json:"task_name"`
		Tasks    []string `json:"tasks"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON")
		return
	}

	var err error
	switch body.Action {
	case "add_task":
		err = h.Pipeline.AddTask(id, body.TaskName)
	case "remove_task":
		err = h.Pipeline.RemoveTask(id, body.TaskName)
	case "reorder":
		err = h.Pipeline.ReorderTasks(id, body.Tasks)
	default:
		writeError(w, http.StatusBadRequest, "unknown action: "+body.Action)
		return
	}

	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handler) handleDeletePipeline(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := h.Pipeline.Delete(id); err != nil {
		writeError(w, http.StatusConflict, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handler) handleStartPipeline(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	p, err := h.Pipeline.Get(id)
	if err != nil {
		writeError(w, http.StatusNotFound, "pipeline not found")
		return
	}
	if len(p.Tasks) == 0 {
		writeError(w, http.StatusBadRequest, "pipeline has no tasks")
		return
	}
	runID, err := h.Runner.Start(id, p.Tasks)
	if err != nil {
		writeError(w, http.StatusConflict, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"run_id": runID})
}

func (h *Handler) handleStopPipeline(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	state, _ := h.Runner.State()
	if state == nil || state.RunningPipeline != id {
		writeError(w, http.StatusBadRequest, "pipeline is not running")
		return
	}
	if err := h.Runner.Stop(); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
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
		size, _ := h.Runner.RunDirSize(e.Name())
		runs = append(runs, runSummary{
			RunID:      e.Name(),
			PipelineID: pipelineID,
			TaskCount:  len(instances),
			Size:       size,
		})
	}
	if runs == nil {
		runs = []runSummary{}
	}
	writeJSON(w, http.StatusOK, runs)
}

func (h *Handler) handleGetRun(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	if r.URL.Query().Get("log") == "1" {
		taskName := r.URL.Query().Get("task")
		if taskName == "" {
			writeError(w, http.StatusBadRequest, "task parameter required for logs")
			return
		}
		stdout, stderr, err := h.Runner.RunLog(id, taskName)
		if err != nil {
			writeError(w, http.StatusNotFound, err.Error())
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
		writeError(w, http.StatusNotFound, "run not found")
		return
	}
	if instances == nil {
		instances = []runner.TaskInstance{}
	}
	writeJSON(w, http.StatusOK, instances)
}

// --- state ---

func (h *Handler) handleState(w http.ResponseWriter, r *http.Request) {
	state, err := h.Runner.State()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
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
func (h *Handler) RecoverOnStartup() {
	if err := h.Runner.RecoverOnStartup(); err != nil {
		fmt.Printf("recovery: %v\n", err)
	}
}
