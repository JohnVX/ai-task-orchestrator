package runner

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// CleanupOldRuns enforces per-pipeline run count limits. For each pipeline,
// if it has more than maxRuns completed runs, the oldest ones are deleted.
// Pipelines that are currently running are skipped entirely.
// Returns the total number of run directories deleted and bytes freed.
func (m *Manager) CleanupOldRuns(maxRuns int) (deleted int, freedBytes int64) {
	if maxRuns <= 0 {
		return 0, 0
	}

	entries, err := os.ReadDir(m.runsDir)
	if err != nil {
		m.logger.Error("cleanup: failed to read runs directory", "dir", m.runsDir, "error", err)
		return 0, 0
	}

	m.stateMu.Lock()
	state, _ := m.readState()
	runningSet := make(map[string]bool, len(state.RunningPipelines))
	for _, rp := range state.RunningPipelines {
		runningSet[rp.PipelineID] = true
	}
	m.stateMu.Unlock()

	type runInfo struct {
		runID      string
		pipelineID string
		size       int64
	}
	var allRuns []runInfo

	for _, e := range entries {
		if !e.IsDir() || !strings.HasPrefix(e.Name(), "run-") {
			continue
		}
		runID := e.Name()
		instances, _ := m.RunInfo(runID)
		if len(instances) == 0 {
			continue
		}
		pipelineID := instances[0].PipelineID
		size, _ := m.RunDirSize(runID)
		allRuns = append(allRuns, runInfo{runID: runID, pipelineID: pipelineID, size: size})
	}

	byPipeline := make(map[string][]runInfo)
	for _, r := range allRuns {
		byPipeline[r.pipelineID] = append(byPipeline[r.pipelineID], r)
	}

	for pipelineID, runs := range byPipeline {
		if runningSet[pipelineID] {
			m.logger.Info("cleanup: skipping running pipeline", "pipeline_id", pipelineID, "run_count", len(runs))
			continue
		}

		if len(runs) <= maxRuns {
			m.logger.Debug("cleanup: pipeline within limit", "pipeline_id", pipelineID, "run_count", len(runs), "limit", maxRuns)
			continue
		}

		sort.Slice(runs, func(i, j int) bool {
			return runs[i].runID < runs[j].runID
		})

		toDelete := runs[:len(runs)-maxRuns]
		m.logger.Info("cleanup: deleting old runs",
			"pipeline_id", pipelineID,
			"total_runs", len(runs),
			"limit", maxRuns,
			"to_delete", len(toDelete),
		)

		for _, r := range toDelete {
			runDir := filepath.Join(m.runsDir, r.runID)
			if err := os.RemoveAll(runDir); err != nil {
				m.logger.Error("cleanup: failed to remove run dir", "run_id", r.runID, "error", err)
				continue
			}
			deleted++
			freedBytes += r.size
			m.logger.Info("cleanup: run deleted",
				"run_id", r.runID,
				"pipeline_id", r.pipelineID,
				"freed_bytes", r.size,
			)
		}
	}

	if deleted > 0 {
		m.logger.Info("cleanup: completed", "deleted_runs", deleted, "freed_bytes", freedBytes)
	}
	return deleted, freedBytes
}
