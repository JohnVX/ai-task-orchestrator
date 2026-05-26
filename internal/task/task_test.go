package task

import (
	"os"
	"path/filepath"
	"testing"
)

func TestParseTaskDescriptorTypeLLMPrompt(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "for-task-orchestrator.txt"),
		[]byte("type: llm-prompt\nstart: ./run.sh\nstop: ./stop.sh\n"), 0644)
	taskType, runCmd, stopCmd := parseTaskDescriptor(dir)
	if taskType != "llm-prompt" {
		t.Fatalf("expected type llm-prompt, got %q", taskType)
	}
	// parseTaskDescriptor parses all fields; ignoring start/stop for llm-prompt is done in Upload
	if runCmd != "./run.sh" {
		t.Fatalf("expected runCmd ./run.sh, got %q", runCmd)
	}
	if stopCmd != "./stop.sh" {
		t.Fatalf("expected stopCmd ./stop.sh, got %q", stopCmd)
	}
}

func TestParseTaskDescriptorTypeSelfContained(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "for-task-orchestrator.txt"),
		[]byte("type: self-contained\nstart: ./custom.sh\n"), 0644)
	taskType, runCmd, stopCmd := parseTaskDescriptor(dir)
	if taskType != "self-contained" {
		t.Fatalf("expected type self-contained, got %q", taskType)
	}
	if runCmd != "./custom.sh" {
		t.Fatalf("expected runCmd ./custom.sh, got %q", runCmd)
	}
	if stopCmd != "" {
		t.Fatalf("expected empty stopCmd, got %q", stopCmd)
	}
}

func TestParseTaskDescriptorNoType(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "for-task-orchestrator.txt"),
		[]byte("start: ./run.sh\n"), 0644)
	taskType, runCmd, stopCmd := parseTaskDescriptor(dir)
	if taskType != "" {
		t.Fatalf("expected empty type, got %q", taskType)
	}
	if runCmd != "./run.sh" {
		t.Fatalf("expected runCmd ./run.sh, got %q", runCmd)
	}
	if stopCmd != "" {
		t.Fatalf("expected empty stopCmd, got %q", stopCmd)
	}
}

func TestParseTaskDescriptorMissingFile(t *testing.T) {
	dir := t.TempDir()
	taskType, runCmd, stopCmd := parseTaskDescriptor(dir)
	if taskType != "" || runCmd != "" || stopCmd != "" {
		t.Fatalf("expected all empty for missing file, got %q/%q/%q", taskType, runCmd, stopCmd)
	}
}
