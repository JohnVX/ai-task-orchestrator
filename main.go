package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"

	"github.com/ai-task-orchestrator/internal/api"
	"github.com/ai-task-orchestrator/internal/pipeline"
	"github.com/ai-task-orchestrator/internal/runner"
	"github.com/ai-task-orchestrator/internal/task"
)

func main() {
	port := flag.Int("port", 8080, "HTTP listen port")
	dataDir := flag.String("data", "./data", "data directory path")
	flag.Parse()

	absDataDir, err := filepath.Abs(*dataDir)
	if err != nil {
		log.Fatalf("resolve data dir: %v", err)
	}

	if err := os.MkdirAll(absDataDir, 0755); err != nil {
		log.Fatalf("create data dir: %v", err)
	}

	taskMgr := task.NewManager(filepath.Join(absDataDir, "tasks"), filepath.Join(absDataDir, "task_meta"), filepath.Join(absDataDir, "pipelines"))
	runMgr := runner.NewManager(filepath.Join(absDataDir, "runs"), absDataDir, taskMgr)
	pipelineMgr := pipeline.NewManager(filepath.Join(absDataDir, "pipelines"), taskMgr, runMgr)
	runMgr.SetPipelineStatusSetter(pipelineMgr)

	h := api.NewHandler(taskMgr, pipelineMgr, runMgr, absDataDir)
	h.RecoverOnStartup()

	addr := fmt.Sprintf(":%d", *port)
	log.Printf("ai-task-orchestrator starting on %s (data: %s)", addr, absDataDir)
	if err := http.ListenAndServe(addr, h.Router()); err != nil {
		log.Fatalf("server: %v", err)
	}
}
