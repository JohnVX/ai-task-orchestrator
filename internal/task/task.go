package task

import (
	"archive/tar"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

// Task types.
const (
	TypeSelfContained = "self-contained"
	TypeLLMPrompt     = "llm-prompt"
)

// Meta holds orchestrator-managed metadata for a single task.
type Meta struct {
	Name              string    `json:"name"`
	PackagePath       string    `json:"package_path"`
	UploadedAt        time.Time `json:"uploaded_at"`
	RunCommand        string    `json:"run_command"`
	StopCommand       string    `json:"stop_command"`
	Type              string    `json:"type,omitempty"`
	LLMAgent          string    `json:"llm_agent,omitempty"`
	ReadmePath        string    `json:"readme_path,omitempty"`
	TimeoutEnabled    bool      `json:"timeout_enabled"`
	TimeoutSeconds    int       `json:"timeout_seconds"`
	OnTimeout         string    `json:"on_timeout"`
	ContinueOnFailure bool      `json:"continue_on_failure"`
	RetryCount        int       `json:"retry_count"`
}

// Manager handles task lifecycle: upload, parse, configure, delete.
type Manager struct {
	tasksDir     string
	taskMetaDir  string
	pipelinesDir string
	logger       *slog.Logger
}

// NewManager creates a Manager. It ensures required directories exist.
func NewManager(tasksDir, taskMetaDir, pipelinesDir string, logger *slog.Logger) *Manager {
	os.MkdirAll(tasksDir, 0755)
	os.MkdirAll(taskMetaDir, 0755)
	return &Manager{tasksDir: tasksDir, taskMetaDir: taskMetaDir, pipelinesDir: pipelinesDir, logger: logger}
}

// --- helpers ---

func (m *Manager) metaPath(name string) string {
	return filepath.Join(m.taskMetaDir, name+".json")
}

func (m *Manager) readMeta(name string) (*Meta, error) {
	f, err := os.Open(m.metaPath(name))
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var meta Meta
	if err := json.NewDecoder(f).Decode(&meta); err != nil {
		return nil, fmt.Errorf("parse meta for %s: %w", name, err)
	}
	return &meta, nil
}

func (m *Manager) writeMeta(meta *Meta) error {
	tmpPath := m.metaPath(meta.Name) + ".tmp"
	f, err := os.Create(tmpPath)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	if err := enc.Encode(meta); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmpPath)
		return err
	}
	return os.Rename(tmpPath, m.metaPath(meta.Name))
}

// --- public methods ---

// Exists returns true if a task with the given name already exists.
func (m *Manager) Exists(name string) bool {
	_, err := os.Stat(m.metaPath(name))
	return err == nil
}

// Pipelines returns pipeline IDs that reference this task.
func (m *Manager) Pipelines(name string) ([]string, error) {
	entries, err := os.ReadDir(m.pipelinesDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var ids []string
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		f, err := os.Open(filepath.Join(m.pipelinesDir, e.Name()))
		if err != nil {
			continue
		}
		var p struct {
			ID    string `json:"id"`
			Tasks []struct {
				Name string `json:"name"`
			} `json:"tasks"`
		}
		if json.NewDecoder(f).Decode(&p) == nil {
			for _, t := range p.Tasks {
				if t.Name == name {
					ids = append(ids, p.ID)
					break
				}
			}
		}
		f.Close()
	}
	return ids, nil
}

// Upload extracts a tar archive into tasks/ and writes metadata.
func (m *Manager) Upload(tarPath string) (*Meta, error) {
	name := strings.TrimSuffix(filepath.Base(tarPath), ".tar")
	if name == "" || name == filepath.Base(tarPath) {
		return nil, fmt.Errorf("invalid tar filename, must be <name>.tar")
	}
	if !validTaskName(name) {
		return nil, fmt.Errorf("invalid task name %q: only letters, digits, hyphens and underscores allowed", name)
	}
	if m.Exists(name) {
		return nil, fmt.Errorf("task %q already exists", name)
	}

	tmpDir, err := os.MkdirTemp(m.tasksDir, ".upload-")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(tmpDir)

	if err := extractTar(tarPath, tmpDir); err != nil {
		return nil, fmt.Errorf("extract tar: %w", err)
	}

	entries, err := os.ReadDir(tmpDir)
	if err != nil {
		return nil, err
	}

	dstDir := filepath.Join(m.tasksDir, name)
	var srcDir string
	if len(entries) == 1 && entries[0].IsDir() {
		srcDir = filepath.Join(tmpDir, entries[0].Name())
	} else {
		srcDir = tmpDir
	}

	if err := os.Rename(srcDir, dstDir); err != nil {
		if copyErr := copyDir(srcDir, dstDir); copyErr != nil {
			return nil, fmt.Errorf("move to %s (rename: %v): %w", dstDir, err, copyErr)
		}
		os.RemoveAll(srcDir)
	}

	var readmePath string
	if _, found := parseReadme(dstDir); found {
		readmePath = "README.md"
	}

	taskType, rc, sc := parseTaskDescriptor(dstDir)

	var runCmd, stopCmd string
	var llmAgent string
	switch taskType {
	case TypeLLMPrompt:
		if _, err := os.Stat(filepath.Join(dstDir, "prompt.md")); os.IsNotExist(err) {
			return nil, fmt.Errorf("llm-prompt task %q requires prompt.md in package", name)
		}
		llmAgent, _ = parseAgentFromDescriptor(dstDir)
		if llmAgent == "" {
			llmAgent = "claude-code"
		}
	case "", TypeSelfContained:
		taskType = TypeSelfContained
		runCmd, stopCmd = "./run.sh", "./stop.sh"
		if rc != "" {
			runCmd = rc
		}
		if sc != "" {
			stopCmd = sc
		}
	default:
		return nil, fmt.Errorf("unknown task type %q in for-task-orchestrator.txt", taskType)
	}

	meta := &Meta{
		Name:        name,
		Type:        taskType,
		PackagePath: filepath.Join("tasks", name),
		UploadedAt:  time.Now().UTC(),
		RunCommand:  runCmd,
		StopCommand: stopCmd,
		LLMAgent:    llmAgent,
		ReadmePath:  readmePath,
	}
	if err := m.writeMeta(meta); err != nil {
		return nil, fmt.Errorf("write meta: %w", err)
	}
	return meta, nil
}

// extractTar extracts a tar archive to dst, guarding against path traversal.
func extractTar(tarPath, dst string) error {
	f, err := os.Open(tarPath)
	if err != nil {
		return err
	}
	defer f.Close()

	tr := tar.NewReader(f)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		clean := filepath.Clean(hdr.Name)
		if filepath.IsAbs(clean) || strings.HasPrefix(clean, ".."+string(os.PathSeparator)) || clean == ".." {
			continue
		}

		target := filepath.Join(dst, clean)
		switch hdr.Typeflag {
		case tar.TypeDir:
			os.MkdirAll(target, 0755)
		case tar.TypeReg:
			os.MkdirAll(filepath.Dir(target), 0755)
			out, err := os.Create(target)
			if err != nil {
				return err
			}
			if _, err := io.Copy(out, tr); err != nil {
				out.Close()
				return err
			}
			out.Close()
			os.Chmod(target, os.FileMode(hdr.Mode)&0777)
		}
	}
	return nil
}

// copyDir copies a directory recursively. Used as fallback when os.Rename fails
// across filesystem boundaries (EXDEV).
func copyDir(src, dst string) error {
	return filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)
		if info.IsDir() {
			return os.MkdirAll(target, info.Mode())
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(target, data, info.Mode())
	})
}

// parseTaskDescriptor reads for-task-orchestrator.txt and extracts start/stop commands.
// First matching line wins; duplicates are silently ignored.
func parseTaskDescriptor(dir string) (taskType, runCmd, stopCmd string) {
	data, err := os.ReadFile(filepath.Join(dir, "for-task-orchestrator.txt"))
	if err != nil {
		return "", "", ""
	}
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if idx := strings.Index(line, "#"); idx >= 0 {
			line = strings.TrimSpace(line[:idx])
			if line == "" {
				continue
			}
		}
		if rest, ok := strings.CutPrefix(line, "type:"); ok && taskType == "" {
			taskType = strings.TrimSpace(rest)
		}
		if rest, ok := strings.CutPrefix(line, "start:"); ok && runCmd == "" {
			runCmd = strings.TrimSpace(rest)
		}
		if rest, ok := strings.CutPrefix(line, "stop:"); ok && stopCmd == "" {
			stopCmd = strings.TrimSpace(rest)
		}
	}
	return
}

func parseAgentFromDescriptor(dir string) (agentName string, found bool) {
	data, err := os.ReadFile(filepath.Join(dir, "for-task-orchestrator.txt"))
	if err != nil {
		return "", false
	}
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if idx := strings.Index(line, "#"); idx >= 0 {
			line = strings.TrimSpace(line[:idx])
			if line == "" {
				continue
			}
		}
		if rest, ok := strings.CutPrefix(line, "agent:"); ok && agentName == "" {
			agentName = strings.TrimSpace(rest)
			found = true
		}
	}
	return
}

// readmePriority lists case-insensitive readme candidates in priority order.
var readmePriority = []string{"README.md", "readme.md", "readme", "readme.txt"}

// parseReadme searches dir for a readme file and returns (content, found).
func parseReadme(dir string) (string, bool) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return "", false
	}

	// Build map: lowercase name → actual name
	lowerToActual := make(map[string]string, len(entries))
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		lowerToActual[strings.ToLower(e.Name())] = e.Name()
	}

	for _, candidate := range readmePriority {
		if actual, ok := lowerToActual[strings.ToLower(candidate)]; ok {
			content, err := os.ReadFile(filepath.Join(dir, actual))
			if err != nil {
				continue
			}
			return string(content), true
		}
	}
	return "", false
}

// ParseReadme looks for a readme file in the task package directory.
func (m *Manager) ParseReadme(name string) (content string, found bool) {
	dir := filepath.Join(m.tasksDir, name)
	return parseReadme(dir)
}

// SetConfig persists task configuration: commands, timeout, continue-on-failure settings.
func (m *Manager) SetConfig(name, runCmd, stopCmd string, timeoutEnabled bool, timeoutSeconds int, onTimeout string, continueOnFailure bool, retryCount int, llmAgent string) error {
	if onTimeout != "" && onTimeout != "skip" && onTimeout != "fail" {
		return fmt.Errorf("on_timeout must be \"skip\", \"fail\", or empty to inherit default")
	}
	meta, err := m.readMeta(name)
	if err != nil {
		return fmt.Errorf("task %q not found: %w", name, err)
	}
	if meta.Type != TypeLLMPrompt {
		meta.RunCommand = runCmd
		meta.StopCommand = stopCmd
	}
	meta.TimeoutEnabled = timeoutEnabled
	meta.TimeoutSeconds = timeoutSeconds
	meta.OnTimeout = onTimeout
	meta.ContinueOnFailure = continueOnFailure
	meta.RetryCount = retryCount
	if llmAgent != "" {
		meta.LLMAgent = llmAgent
	}
	return m.writeMeta(meta)
}

// Delete removes a task's package directory and metadata file.
func (m *Manager) Delete(name string) error {
	if !m.Exists(name) {
		return fmt.Errorf("task %q not found", name)
	}
	ids, err := m.Pipelines(name)
	if err != nil {
		return err
	}
	if len(ids) > 0 {
		return fmt.Errorf("task %q is used by pipelines: %s", name, strings.Join(ids, ", "))
	}
	if err := os.RemoveAll(filepath.Join(m.tasksDir, name)); err != nil {
		return err
	}
	os.Remove(m.metaPath(name))
	return nil
}

// All returns metadata for every registered task.
func (m *Manager) All() ([]Meta, error) {
	entries, err := os.ReadDir(m.taskMetaDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var tasks []Meta
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		name := strings.TrimSuffix(e.Name(), ".json")
		meta, err := m.readMeta(name)
		if err != nil {
			if m.logger != nil {
				m.logger.Warn("task: skipping unreadable meta file", "name", name, "error", err)
			}
			continue
		}
		tasks = append(tasks, *meta)
	}
	return tasks, nil
}

// Export creates a tar archive of the task's package directory and returns its path.
// The caller is responsible for removing the temp file after use.
func (m *Manager) Export(name string) (string, error) {
	meta, err := m.readMeta(name)
	if err != nil {
		return "", fmt.Errorf("task %q not found: %w", name, err)
	}

	taskDir := filepath.Join(m.tasksDir, name)
	if _, err := os.Stat(taskDir); err != nil {
		return "", fmt.Errorf("task package dir not found: %w", err)
	}

	tmpFile, err := os.CreateTemp("", "task-export-*.tar")
	if err != nil {
		return "", fmt.Errorf("create temp file: %w", err)
	}
	tw := tar.NewWriter(tmpFile)

	err = filepath.Walk(taskDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(taskDir, path)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil
		}
		if strings.Contains(rel, "..") {
			return nil
		}

		if !info.Mode().IsRegular() && !info.IsDir() {
			return nil
		}

		hdr, err := tar.FileInfoHeader(info, "")
		if err != nil {
			return err
		}
		hdr.Name = rel

		if err := tw.WriteHeader(hdr); err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}

		f, err := os.Open(path)
		if err != nil {
			return err
		}
		_, err = io.Copy(tw, f)
		f.Close()
		return err
	})

	closeErr := tw.Close()
	tmpFile.Close()

	if err != nil {
		os.Remove(tmpFile.Name())
		return "", fmt.Errorf("create tar: %w", err)
	}
	if closeErr != nil {
		os.Remove(tmpFile.Name())
		return "", fmt.Errorf("close tar: %w", closeErr)
	}

	_ = meta // name validated via readMeta
	return tmpFile.Name(), nil
}

// Get returns metadata for a specific task.
func (m *Manager) Get(name string) (*Meta, error) {
	return m.readMeta(name)
}

var taskNameRe = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_-]*$`)

func validTaskName(name string) bool {
	return taskNameRe.MatchString(name)
}
