package agent

import (
	"sync"
	"testing"
)

// TestConcurrentAccess verifies the registry is safe under concurrent
// read and write access (regression test for missing RWMutex).
func TestConcurrentAccess(t *testing.T) {
	t.Parallel()

	var wg sync.WaitGroup

	// 20 concurrent readers
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				Get("claude-code")
				Get("opencode")
				Get("nonexistent")
			}
		}()
	}

	// 1 concurrent writer
	wg.Add(1)
	go func() {
		defer wg.Done()
		for j := 0; j < 5; j++ {
			Register("race-test-agent", &claudeCodeAgent{})
		}
	}()

	wg.Wait()

	// Verify writer's registration was visible
	a, err := Get("race-test-agent")
	if err != nil {
		t.Fatalf("registered agent not found after concurrent access: %v", err)
	}
	if a.Name() != "claude-code" {
		t.Fatalf("expected 'claude-code', got %q", a.Name())
	}
}
