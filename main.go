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
	"syscall"
	"time"

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

	taskMgr := task.NewManager(filepath.Join(absDataDir, "tasks"), filepath.Join(absDataDir, "task_meta"), filepath.Join(absDataDir, "pipelines"))
	runMgr := runner.NewManager(filepath.Join(absDataDir, "runs"), absDataDir, taskMgr, slogger)
	pipelineMgr := pipeline.NewManager(filepath.Join(absDataDir, "pipelines"), taskMgr, runMgr)
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

	h := api.NewHandler(taskMgr, pipelineMgr, runMgr, absDataDir, tmpl, http.FS(staticFS))
	if err := h.RecoverOnStartup(); err != nil {
		slogger.Error("recovery failed", "error", err)
		os.Exit(1)
	}

	go runScheduler(pipelineMgr, runMgr, slogger)

	addr := fmt.Sprintf(":%d", *port)
	srv := &http.Server{Addr: addr, Handler: h.Router()}

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

func runScheduler(pipeMgr *pipeline.Manager, runMgr *runner.Manager, logger *slog.Logger) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	lastRun := make(map[string]time.Time)

	for range ticker.C {
		pipes, err := pipeMgr.All()
		if err != nil {
			continue
		}
		now := time.Now()
		activeIDs := make(map[string]bool, len(pipes))
		for _, p := range pipes {
			activeIDs[p.ID] = true
			if p.Schedule == "" || p.Status == pipeline.StatusRunning {
				continue
			}
			if len(p.Tasks) == 0 {
				continue
			}
			if !runner.MatchCron(p.Schedule, now) {
				continue
			}
			minuteKey := now.Truncate(time.Minute)
			if last, ok := lastRun[p.ID]; ok && !last.Before(minuteKey) {
				continue
			}
			lastRun[p.ID] = minuteKey

			logger.Info("scheduled pipeline triggered", "pipeline_id", p.ID, "schedule", p.Schedule)
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
			if _, err := runMgr.Start(p.ID, runTasks, p.WebhookURL, p.Name, resolveLoopCount(p.LoopCount)); err != nil {
				logger.Error("scheduled pipeline start failed", "pipeline_id", p.ID, "error", err)
			}
		}
		for id := range lastRun {
			if !activeIDs[id] {
				delete(lastRun, id)
			}
		}
	}
}


func resolveLoopCount(lc *int) int {
	if lc == nil {
		return 1
	}
	return *lc
}
