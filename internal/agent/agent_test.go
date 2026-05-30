package agent

import (
	"os"
	"path/filepath"
	"testing"
)

func TestRegistryContainsBothAgents(t *testing.T) {
	t.Parallel()
	names := []string{"claude-code", "opencode"}
	for _, name := range names {
		a, err := Get(name)
		if err != nil {
			t.Errorf("agent %q not found in registry: %v", name, err)
		}
		if a.Name() != name {
			t.Errorf("expected name %q, got %q", name, a.Name())
		}
	}
}

func TestGetUnknownAgent(t *testing.T) {
	t.Parallel()
	_, err := Get("unknown-agent")
	if err == nil {
		t.Error("expected error for unknown agent")
	}
}

func TestMustGetPanicsOnUnknown(t *testing.T) {
	t.Parallel()
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic for MustGet with unknown agent")
		}
	}()
	MustGet("nonexistent")
}

func TestOpencodeAgentName(t *testing.T) {
	t.Parallel()
	a := &opencodeAgent{}
	if a.Name() != "opencode" {
		t.Errorf("expected name 'opencode', got %q", a.Name())
	}
}

func TestOpencodeAgentBuildCommandReadsPrompt(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	promptFile := filepath.Join(dir, "prompt.md")
	content := "List all files in the current directory.\n"
	if err := os.WriteFile(promptFile, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	a := &opencodeAgent{}
	cmd, err := a.BuildCommand(promptFile, dir)
	if err != nil {
		t.Fatalf("BuildCommand failed: %v", err)
	}

	if cmd.Dir != dir {
		t.Errorf("expected cmd.Dir=%q, got %q", dir, cmd.Dir)
	}
	if cmd.Path == "" {
		t.Error("expected cmd.Path to be set")
	}

	expectedArgs := []string{"run", "--dangerously-skip-permissions", "--dir", dir}
	for i, arg := range expectedArgs {
		if cmd.Args[i+1] != arg {
			t.Errorf("arg[%d]: expected %q, got %q", i+1, arg, cmd.Args[i+1])
		}
	}
	promptArg := cmd.Args[len(cmd.Args)-1]
	if len(promptArg) == 0 || promptArg[:1] != "E" {
		t.Errorf("prompt arg should start with directive prefix, got %q...", promptArg[:40])
	}
	if promptArg[len(promptArg)-len(content):] != content {
		t.Errorf("prompt arg should end with original content, got %q...", promptArg[len(promptArg)-20:])
	}
}

func TestOpencodeAgentBuildCommandMissingFile(t *testing.T) {
	t.Parallel()
	a := &opencodeAgent{}
	_, err := a.BuildCommand("/nonexistent/path/prompt.md", "/tmp")
	if err == nil {
		t.Error("expected error for missing prompt file")
	}
}

func TestRegisterCustomAgent(t *testing.T) {
	t.Parallel()
	custom := &claudeCodeAgent{}
	Register("custom-test", custom)
	a, err := Get("custom-test")
	if err != nil {
		t.Fatalf("custom agent not found: %v", err)
	}
	if a.Name() != "claude-code" {
		t.Errorf("expected underlying name 'claude-code', got %q", a.Name())
	}
}