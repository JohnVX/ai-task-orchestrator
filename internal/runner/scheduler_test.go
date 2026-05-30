package runner

import (
	"log/slog"
	"testing"
	"time"
)

// mockStartRecorder records Start calls for verification.
type mockStartRecorder struct {
	started []string
}

func (m *mockStartRecorder) Start(pipelineID string, _ []RunTask, _, _ string, _ int) (string, error) {
	m.started = append(m.started, pipelineID)
	return "run-000001", nil
}

type mockChecker struct {
	pipelines []ScheduledPipeline
}

func (m *mockChecker) All() ([]ScheduledPipeline, error) {
	return m.pipelines, nil
}

func TestSchedulerNoSchedule(t *testing.T) {
	t.Parallel()
	s := NewScheduler(
		&mockChecker{pipelines: []ScheduledPipeline{
			{ID: "p1", Name: "no-sched", Status: "idle", Tasks: []RunTask{{Name: "t1"}}},
		}},
		&mockStartRecorder{},
		slog.Default(),
	)
	triggered := s.Tick(time.Now())
	if len(triggered) != 0 {
		t.Fatalf("expected 0 triggered, got %v", triggered)
	}
}

func TestSchedulerNoTasks(t *testing.T) {
	t.Parallel()
	s := NewScheduler(
		&mockChecker{pipelines: []ScheduledPipeline{
			{ID: "p2", Name: "no-tasks", Schedule: "* * * * *", Status: "idle"},
		}},
		&mockStartRecorder{},
		slog.Default(),
	)
	triggered := s.Tick(time.Now())
	if len(triggered) != 0 {
		t.Fatalf("expected 0 triggered, got %v", triggered)
	}
}

func TestSchedulerRunning(t *testing.T) {
	t.Parallel()
	s := NewScheduler(
		&mockChecker{pipelines: []ScheduledPipeline{
			{ID: "p3", Name: "running-pipe", Schedule: "* * * * *", Status: "running", Tasks: []RunTask{{Name: "t1"}}},
		}},
		&mockStartRecorder{},
		slog.Default(),
	)
	triggered := s.Tick(time.Now())
	if len(triggered) != 0 {
		t.Fatalf("expected 0 triggered for running pipeline, got %v", triggered)
	}
}

func TestSchedulerTriggers(t *testing.T) {
	t.Parallel()
	rec := &mockStartRecorder{}
	s := NewScheduler(
		&mockChecker{pipelines: []ScheduledPipeline{
			{ID: "p4", Name: "trigger-me", Schedule: "* * * * *", Status: "idle", Tasks: []RunTask{{Name: "t1"}}},
		}},
		rec,
		slog.Default(),
	)

	now := time.Date(2026, 5, 30, 10, 0, 0, 0, time.UTC)
	triggered := s.Tick(now)
	if len(triggered) != 1 || triggered[0] != "p4" {
		t.Fatalf("expected triggered=[p4], got %v", triggered)
	}
	if len(rec.started) != 1 || rec.started[0] != "p4" {
		t.Fatalf("expected Start called with p4, got %v", rec.started)
	}
}

func TestSchedulerThrottlesSameMinute(t *testing.T) {
	t.Parallel()
	rec := &mockStartRecorder{}
	s := NewScheduler(
		&mockChecker{pipelines: []ScheduledPipeline{
			{ID: "p5", Name: "throttle-me", Schedule: "* * * * *", Status: "idle", Tasks: []RunTask{{Name: "t1"}}},
		}},
		rec,
		slog.Default(),
	)

	now := time.Date(2026, 5, 30, 10, 0, 0, 0, time.UTC)
	s.Tick(now)

	// Second tick same minute should not trigger
	triggered := s.Tick(now)
	if len(triggered) != 0 {
		t.Fatalf("expected 0 triggered (throttled), got %v", triggered)
	}
	if len(rec.started) != 1 {
		t.Fatalf("expected Start called once, got %d calls", len(rec.started))
	}

	// Next minute should trigger again
	nextMin := time.Date(2026, 5, 30, 10, 1, 0, 0, time.UTC)
	triggered = s.Tick(nextMin)
	if len(triggered) != 1 || triggered[0] != "p5" {
		t.Fatalf("expected triggered=[p5] next minute, got %v", triggered)
	}
	if len(rec.started) != 2 {
		t.Fatalf("expected Start called twice, got %d calls", len(rec.started))
	}
}

func TestSchedulerCronNotMatch(t *testing.T) {
	t.Parallel()
	s := NewScheduler(
		&mockChecker{pipelines: []ScheduledPipeline{
			{ID: "p6", Name: "no-match", Schedule: "30 9 * * 1-5", Status: "idle", Tasks: []RunTask{{Name: "t1"}}},
		}},
		&mockStartRecorder{},
		slog.Default(),
	)

	// Saturday at 10:00 should not match 30 9 * * 1-5
	now := time.Date(2026, 5, 30, 10, 0, 0, 0, time.UTC) // Saturday
	triggered := s.Tick(now)
	if len(triggered) != 0 {
		t.Fatalf("expected 0 triggered (cron mismatch), got %v", triggered)
	}
}

func TestSchedulerCleansDeletedPipelines(t *testing.T) {
	t.Parallel()
	checker := &mockChecker{
		pipelines: []ScheduledPipeline{
			{ID: "p7", Name: "will-be-deleted", Schedule: "* * * * *", Status: "idle", Tasks: []RunTask{{Name: "t1"}}},
		},
	}
	s := NewScheduler(checker, &mockStartRecorder{}, slog.Default())

	now := time.Date(2026, 5, 30, 10, 0, 0, 0, time.UTC)
	s.Tick(now)

	// Pipeline removed from checker
	checker.pipelines = nil

	// Tick should not panic and should clean up lastRun entry
	triggered := s.Tick(now.Add(61 * time.Second))
	if len(triggered) != 0 {
		t.Fatalf("expected 0 triggered after delete, got %v", triggered)
	}
}

func TestSchedulerMultiplePipelines(t *testing.T) {
	t.Parallel()
	rec := &mockStartRecorder{}
	s := NewScheduler(
		&mockChecker{pipelines: []ScheduledPipeline{
			{ID: "p8", Name: "match-1", Schedule: "* * * * *", Status: "idle", Tasks: []RunTask{{Name: "t1"}}},
			{ID: "p9", Name: "no-sched", Status: "idle", Tasks: []RunTask{{Name: "t1"}}},
			{ID: "p10", Name: "running-pipe", Schedule: "* * * * *", Status: "running", Tasks: []RunTask{{Name: "t1"}}},
			{ID: "p11", Name: "match-2", Schedule: "* * * * *", Status: "idle", Tasks: []RunTask{{Name: "t2"}}},
		}},
		rec,
		slog.Default(),
	)

	now := time.Date(2026, 5, 30, 10, 0, 0, 0, time.UTC)
	triggered := s.Tick(now)
	if len(triggered) != 2 || triggered[0] != "p8" || triggered[1] != "p11" {
		t.Fatalf("expected triggered=[p8, p11], got %v", triggered)
	}
	if len(rec.started) != 2 {
		t.Fatalf("expected Start called 2 times, got %d", len(rec.started))
	}
}
