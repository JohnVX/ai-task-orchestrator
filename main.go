package main

import (
	"context"
	"embed"
	"flag"
	"fmt"
	"html/template"
	"io/fs"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/ai-task-orchestrator/internal/agent"
	"github.com/ai-task-orchestrator/internal/api"
	"github.com/ai-task-orchestrator/internal/logger"
	"github.com/ai-task-orchestrator/internal/pipeline"
	"github.com/ai-task-orchestrator/internal/runner"
	"github.com/ai-task-orchestrator/internal/task"
)

//go:embed web/templates/index.html
var indexHTML string

//go:embed web/static/*
var staticFiles embed.FS

func main() {
	port := flag.Int("port", 8080, "HTTP listen port")
	dataDir := flag.String("data", "./data", "data directory path")
	logLevel := flag.String("log-level", "info", "log level: debug, info, warn, error")
	maxRuns := flag.Int("max-runs", 100, "max completed runs per pipeline (0=unlimited)")
	llmAgent := flag.String("llm-agent", "claude-code", "LLM agent for prompt tasks (claude-code)")
	flag.Parse()

	absDataDir, err := filepath.Abs(*dataDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "resolve data dir: %v\n", err)
		os.Exit(1)
	}

	if err := os.MkdirAll(absDataDir, 0755); err != nil {
		fmt.Fprintf(os.Stderr, "create data dir: %v\n", err)
		os.Exit(1)
	}

	compressed, deleted := logger.Rotate(absDataDir, "orchestrator.log*")

	slogger, err := logger.New(*logLevel, filepath.Join(absDataDir, "orchestrator.log"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "init logger: %v\n", err)
		os.Exit(1)
	}
	slogger.Info("log rotation completed", "compressed", compressed, "deleted", deleted)

	// Clean up leftover .tmp files from crash-safe writes
	tmpCleaned := cleanTempFiles(absDataDir)
	if tmpCleaned > 0 {
		slogger.Info("cleaned stale temp files", "count", tmpCleaned)
	}

	taskMgr := task.NewManager(filepath.Join(absDataDir, "tasks"), filepath.Join(absDataDir, "task_meta"), filepath.Join(absDataDir, "pipelines"), slogger)
	agt, err := agent.Get(*llmAgent)
	if err != nil {
		slogger.Warn("llm agent not available, llm-prompt tasks will fail at runtime", "agent", *llmAgent, "error", err)
		agt = nil
	} else {
		slogger.Info("llm agent resolved", "agent", agt.Name())
	}

	runMgr := runner.NewManager(filepath.Join(absDataDir, "runs"), absDataDir, taskMgr, slogger, agt)
	pipelineMgr := pipeline.NewManager(filepath.Join(absDataDir, "pipelines"), taskMgr, runMgr, slogger)
	runMgr.SetPipelineStatusSetter(pipelineMgr)

	delRuns, freedBytes := runMgr.CleanupOldRuns(*maxRuns)
	if delRuns > 0 {
		slogger.Info("startup cleanup completed", "deleted_runs", delRuns, "freed_bytes", freedBytes)
	}

	go func() {
		tk := time.NewTicker(24 * time.Hour)
		defer tk.Stop()
		for range tk.C {
			c, d := logger.Rotate(absDataDir, "orchestrator.log*")
			if c+d > 0 {
				slogger.Info("log rotation", "compressed", c, "deleted", d)
			}
			del, freed := runMgr.CleanupOldRuns(*maxRuns)
			if del > 0 {
				slogger.Info("periodic cleanup completed", "deleted_runs", del, "freed_bytes", freed)
			}
		}
	}()

	tmpl := template.Must(template.New("index").Parse(indexHTML))
	staticFS, _ := fs.Sub(staticFiles, "web/static")

	h := api.NewHandler(taskMgr, pipelineMgr, runMgr, absDataDir, tmpl, http.FS(staticFS), slogger)
	if err := h.RecoverOnStartup(); err != nil {
		slogger.Error("recovery failed", "error", err)
		os.Exit(1)
	}

	go runScheduler(pipelineMgr, runMgr, slogger)

	addr := fmt.Sprintf(":%d", *port)
	srv := &http.Server{
		Addr:              addr,
		Handler:           requestLogger(slogger, h.Router()),
		ReadTimeout:       60 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       120 * time.Second,
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		sig := <-sigCh
		slogger.Info("shutting down", "signal", sig.String())

		runMgr.StopAll()

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := srv.Shutdown(ctx); err != nil {
			slogger.Error("shutdown", "error", err)
		}
	}()

	slogger.Info("ai-task-orchestrator starting", "addr", addr, "data", absDataDir)
	if err := srv.ListenAndServe(); err != http.ErrServerClosed {
		slogger.Error("server failed", "error", err)
		os.Exit(1)
	}
	slogger.Info("server stopped")
}

// pipeAdapter adapts *pipeline.Manager to runner.ScheduleChecker.
type pipeAdapter struct {
	mgr *pipeline.Manager
}

func (a *pipeAdapter) All() ([]runner.ScheduledPipeline, error) {
	pipes, err := a.mgr.All()
	if err != nil {
		return nil, err
	}
	sps := make([]runner.ScheduledPipeline, len(pipes))
	for i, p := range pipes {
		tasks := make([]runner.RunTask, len(p.Tasks))
		for j, ref := range p.Tasks {
			tasks[j] = runner.RunTask{
				Name:              ref.Name,
				TimeoutSeconds:    ref.TimeoutSeconds,
				OnTimeout:         ref.OnTimeout,
				ContinueOnFailure: ref.ContinueOnFailure,
				RetryCount:        ref.RetryCount,
				Stage:             ref.Stage,
			}
		}
		sps[i] = runner.ScheduledPipeline{
			ID:         p.ID,
			Name:       p.Name,
			Schedule:   p.Schedule,
			Status:     p.Status,
			WebhookURL: p.WebhookURL,
			LoopCount:  p.LoopCount,
			Tasks:      tasks,
		}
	}
	return sps, nil
}

func runScheduler(pipeMgr *pipeline.Manager, runMgr *runner.Manager, logger *slog.Logger) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	sched := runner.NewScheduler(&pipeAdapter{mgr: pipeMgr}, runMgr, logger)

	for range ticker.C {
		sched.Tick(time.Now())
	}
}

// cleanTempFiles removes leftover .tmp files in data subdirectories
// that may remain after a crash during crash-safe writes.
func cleanTempFiles(dataDir string) int {
	var count int
	for _, sub := range []string{"pipelines", "task_meta", "runs"} {
		dir := filepath.Join(dataDir, sub)
		entries, err := os.ReadDir(dir)
		if err != nil {
			continue
		}
		for _, e := range entries {
			if strings.HasSuffix(e.Name(), ".tmp") {
				os.Remove(filepath.Join(dir, e.Name()))
				count++
			}
		}
	}
	return count
}

type responseWriter struct {
	http.ResponseWriter
	status int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.status = code
	rw.ResponseWriter.WriteHeader(code)
}

func requestLogger(logger *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rw := &responseWriter{ResponseWriter: w, status: 200}
		t0 := time.Now()
		next.ServeHTTP(rw, r)
		dur := time.Since(t0)
		if r.URL.Path != "/" && !strings.HasPrefix(r.URL.Path, "/static/") {
			logger.Info("http request",
				"method", r.Method,
				"path", r.URL.Path,
				"status", rw.status,
				"duration_ms", dur.Milliseconds(),
				"remote", r.RemoteAddr,
			)
		}
	})
}
