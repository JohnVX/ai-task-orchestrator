package main

import (
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/ai-task-orchestrator/internal/api"
	"github.com/ai-task-orchestrator/internal/logger"
	"github.com/ai-task-orchestrator/internal/pipeline"
	"github.com/ai-task-orchestrator/internal/runner"
	"github.com/ai-task-orchestrator/internal/task"
)

func main() {
	port := flag.Int("port", 8080, "HTTP listen port")
	dataDir := flag.String("data", "./data", "data directory path")
	logLevel := flag.String("log-level", "info", "log level: debug, info, warn, error")
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

	go func() {
		tk := time.NewTicker(24 * time.Hour)
		defer tk.Stop()
		for range tk.C {
			c, d := logger.Rotate(absDataDir, "orchestrator.log*")
			if c+d > 0 {
				slogger.Info("log rotation", "compressed", c, "deleted", d)
			}
		}
	}()

	taskMgr := task.NewManager(filepath.Join(absDataDir, "tasks"), filepath.Join(absDataDir, "task_meta"), filepath.Join(absDataDir, "pipelines"))
	runMgr := runner.NewManager(filepath.Join(absDataDir, "runs"), absDataDir, taskMgr, slogger)
	pipelineMgr := pipeline.NewManager(filepath.Join(absDataDir, "pipelines"), taskMgr, runMgr)
	runMgr.SetPipelineStatusSetter(pipelineMgr)

	h := api.NewHandler(taskMgr, pipelineMgr, runMgr, absDataDir)
	if err := h.RecoverOnStartup(); err != nil {
		slogger.Error("recovery failed", "error", err)
		os.Exit(1)
	}

	go runScheduler(pipelineMgr, runMgr, slogger)

	addr := fmt.Sprintf(":%d", *port)
	slogger.Info("ai-task-orchestrator starting", "addr", addr, "data", absDataDir)
	if err := http.ListenAndServe(addr, h.Router()); err != nil {
		slogger.Error("server failed", "error", err)
		os.Exit(1)
	}
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
		for _, p := range pipes {
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
			if _, err := runMgr.Start(p.ID, p.Tasks); err != nil {
				logger.Error("scheduled pipeline start failed", "pipeline_id", p.ID, "error", err)
			}
		}
	}
}
