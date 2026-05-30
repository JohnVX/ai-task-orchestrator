package runner

import (
	"log/slog"
	"time"
)

// ScheduleChecker is the interface the scheduler needs from pipeline management.
type ScheduleChecker interface {
	All() ([]ScheduledPipeline, error)
}

// ScheduledPipeline is a subset of pipeline.Pipeline fields needed by the scheduler.
type ScheduledPipeline struct {
	ID         string
	Name       string
	Schedule   string
	Status     string
	WebhookURL string
	LoopCount  *int
	Tasks      []RunTask
}

// Starter abstracts starting a pipeline, making Scheduler testable.
type Starter interface {
	Start(pipelineID string, tasks []RunTask, webhookURL string, pipelineName string, loopCount int) (string, error)
}

// Scheduler periodically checks pipelines with cron schedules and starts them.
type Scheduler struct {
	pipes  ScheduleChecker
	runner Starter
	logger *slog.Logger

	lastRun map[string]time.Time
}

// NewScheduler creates a Scheduler.
func NewScheduler(pipes ScheduleChecker, runner Starter, logger *slog.Logger) *Scheduler {
	return &Scheduler{
		pipes:   pipes,
		runner:  runner,
		logger:  logger,
		lastRun: make(map[string]time.Time),
	}
}

// Tick checks all pipelines and starts any whose cron expression matches now.
// Returns the IDs of pipelines that were triggered.
func (s *Scheduler) Tick(now time.Time) []string {
	all, err := s.pipes.All()
	if err != nil {
		return nil
	}

	var triggered []string
	activeIDs := make(map[string]bool, len(all))

	for _, p := range all {
		activeIDs[p.ID] = true

		if p.Schedule == "" || p.Status == "running" || len(p.Tasks) == 0 {
			continue
		}
		if !MatchCron(p.Schedule, now) {
			continue
		}
		minuteKey := now.Truncate(time.Minute)
		if last, ok := s.lastRun[p.ID]; ok && !last.Before(minuteKey) {
			continue
		}
		s.lastRun[p.ID] = minuteKey

		s.logger.Info("scheduled pipeline triggered", "pipeline_id", p.ID, "schedule", p.Schedule)
		if _, err := s.runner.Start(p.ID, p.Tasks, p.WebhookURL, p.Name, ResolveLoopCount(p.LoopCount)); err != nil {
			s.logger.Error("scheduled pipeline start failed", "pipeline_id", p.ID, "error", err)
			continue
		}
		triggered = append(triggered, p.ID)
	}

	// Clean up entries for deleted pipelines
	for id := range s.lastRun {
		if !activeIDs[id] {
			delete(s.lastRun, id)
		}
	}
	return triggered
}
