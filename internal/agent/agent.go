// Package agent provides an abstraction for LLM agent execution.
// Agents are identified by name and registered at init time.
package agent

import (
	"fmt"
	"os/exec"
)

// Agent executes prompt-based tasks through a specific LLM agent binary.
type Agent interface {
	// Name returns the agent identifier string (matches --llm-agent flag value).
	Name() string

	// BuildCommand returns an *exec.Cmd configured to execute a prompt file.
	// promptFile is the absolute path to the prompt.md in the task package.
	// workDir is the task package directory (set as cmd.Dir).
	// Returns an error if the agent binary is not found in PATH.
	BuildCommand(promptFile, workDir string) (*exec.Cmd, error)
}

var registry = map[string]Agent{
	"claude-code": &claudeCodeAgent{},
}

// Register adds an agent to the registry.
func Register(name string, a Agent) {
	registry[name] = a
}

// Get looks up an agent by name. Returns an error if not found.
func Get(name string) (Agent, error) {
	a, ok := registry[name]
	if !ok {
		return nil, fmt.Errorf("unknown agent %q", name)
	}
	return a, nil
}

// MustGet is like Get but panics on error. Useful for tests and startup.
func MustGet(name string) Agent {
	a, err := Get(name)
	if err != nil {
		panic(err)
	}
	return a
}

type claudeCodeAgent struct{}

func (a *claudeCodeAgent) Name() string { return "claude-code" }

func (a *claudeCodeAgent) BuildCommand(promptFile, workDir string) (*exec.Cmd, error) {
	if _, err := exec.LookPath("claude"); err != nil {
		return nil, fmt.Errorf("claude not found in PATH: %w", err)
	}
	cmd := exec.Command("claude", "-p", promptFile)
	cmd.Dir = workDir
	return cmd, nil
}
