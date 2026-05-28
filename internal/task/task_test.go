package task

import (
	"archive/tar"
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// ===== validTaskName =====

func TestValidTaskName(t *testing.T) {
	tests := []struct {
		name  string
		valid bool
	}{
		{"my-task", true},
		{"my_task", true},
		{"myTask1", true},
		{"1-start-with-digit", true},
		{"a", true},
		{"", false},
		{"-leading-hyphen", false},
		{"_leading-underscore", false},
		{"has space", false},
		{"has/slash", false},
		{"has.dot", false},
		{"中文", false},
	}
	for _, tt := range tests {
		got := validTaskName(tt.name)
		if got != tt.valid {
			t.Errorf("validTaskName(%q) = %v, want %v", tt.name, got, tt.valid)
		}
	}
}

// ===== parseReadme =====

func TestParseReadmeFound(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "README.md"), []byte("# My Task\nContent"), 0644)
	content, found := parseReadme(dir)
	if !found {
		t.Fatal("expected readme to be found")
	}
	if !strings.Contains(content, "My Task") {
		t.Fatalf("expected readme content, got %q", content)
	}
}

func TestParseReadmeLowercase(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "readme.md"), []byte("# lower"), 0644)
	content, found := parseReadme(dir)
	if !found {
		t.Fatal("expected readme.md to be found")
	}
	if !strings.Contains(content, "lower") {
		t.Fatalf("expected content, got %q", content)
	}
}

func TestParseReadmeNoExtension(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "readme"), []byte("no ext"), 0644)
	content, found := parseReadme(dir)
	if !found {
		t.Fatal("expected readme to be found")
	}
	if !strings.Contains(content, "no ext") {
		t.Fatalf("expected content, got %q", content)
	}
}

func TestParseReadmeNotFound(t *testing.T) {
	dir := t.TempDir()
	_, found := parseReadme(dir)
	if found {
		t.Fatal("expected no readme in empty dir")
	}
}

func TestParseReadmeSkipsDirs(t *testing.T) {
	dir := t.TempDir()
	os.MkdirAll(filepath.Join(dir, "README.md"), 0755) // directory named README.md
	_, found := parseReadme(dir)
	if found {
		t.Fatal("should not match directories named README.md")
	}
}

func TestParseReadmePriority(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "readme.txt"), []byte("txt content"), 0644)
	os.WriteFile(filepath.Join(dir, "README.md"), []byte("md content"), 0644)
	content, found := parseReadme(dir)
	if !found {
		t.Fatal("expected readme to be found")
	}
	if content != "md content" {
		t.Fatalf("expected README.md to take priority, got %q", content)
	}
}

// ===== parseTaskDescriptor =====

func TestParseTaskDescriptorTypeLLMPrompt(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "for-task-orchestrator.txt"),
		[]byte("type: llm-prompt\nstart: ./run.sh\nstop: ./stop.sh\n"), 0644)
	taskType, runCmd, stopCmd := parseTaskDescriptor(dir)
	if taskType != "llm-prompt" {
		t.Fatalf("expected type llm-prompt, got %q", taskType)
	}
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

func TestParseTaskDescriptorComments(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "for-task-orchestrator.txt"),
		[]byte("# this is a comment\ntype: llm-prompt\nstart: ./run.sh  # inline comment\n"), 0644)
	taskType, runCmd, _ := parseTaskDescriptor(dir)
	if taskType != "llm-prompt" {
		t.Fatalf("expected type llm-prompt, got %q", taskType)
	}
	if runCmd != "./run.sh" {
		t.Fatalf("expected runCmd ./run.sh, got %q", runCmd)
	}
}

func TestParseTaskDescriptorFirstMatchWins(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "for-task-orchestrator.txt"),
		[]byte("start: ./first.sh\nstart: ./second.sh\n"), 0644)
	_, runCmd, _ := parseTaskDescriptor(dir)
	if runCmd != "./first.sh" {
		t.Fatalf("expected first match ./first.sh, got %q", runCmd)
	}
}

func TestParseTaskDescriptorBlankLines(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "for-task-orchestrator.txt"),
		[]byte("\n\nstart: ./run.sh\n\n"), 0644)
	_, runCmd, _ := parseTaskDescriptor(dir)
	if runCmd != "./run.sh" {
		t.Fatalf("expected ./run.sh, got %q", runCmd)
	}
}

// ===== extractTar =====

func makeTestTar(t *testing.T, files map[string]string) string {
	t.Helper()
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for name, content := range files {
		hdr := &tar.Header{Name: name, Mode: 0644, Size: int64(len(content))}
		if err := tw.WriteHeader(hdr); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write([]byte(content)); err != nil {
			t.Fatal(err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "test.tar")
	os.WriteFile(path, buf.Bytes(), 0644)
	return path
}

func TestExtractTarBasic(t *testing.T) {
	dst := t.TempDir()
	tarPath := makeTestTar(t, map[string]string{
		"file1.txt": "hello",
		"sub/file2.txt": "world",
	})
	if err := extractTar(tarPath, dst); err != nil {
		t.Fatal(err)
	}
	data1, _ := os.ReadFile(filepath.Join(dst, "file1.txt"))
	if string(data1) != "hello" {
		t.Fatalf("expected 'hello', got %q", string(data1))
	}
	data2, _ := os.ReadFile(filepath.Join(dst, "sub", "file2.txt"))
	if string(data2) != "world" {
		t.Fatalf("expected 'world', got %q", string(data2))
	}
}

func TestExtractTarPathTraversalRejected(t *testing.T) {
	dst := t.TempDir()
	tarPath := makeTestTar(t, map[string]string{
		"../escape.txt": "bad",
	})
	if err := extractTar(tarPath, dst); err != nil {
		t.Fatal(err)
	}
	// escaped file should not exist
	if _, err := os.Stat(filepath.Join(dst, "..", "escape.txt")); !os.IsNotExist(err) {
		t.Fatal("path traversal file should not have been extracted")
	}
}

func TestExtractTarAbsolutePathRejected(t *testing.T) {
	dst := t.TempDir()
	tarPath := makeTestTar(t, map[string]string{
		"/etc/passwd": "bad",
	})
	if err := extractTar(tarPath, dst); err != nil {
		t.Fatal(err)
	}
}

func TestExtractTarInvalidTar(t *testing.T) {
	dst := t.TempDir()
	badPath := filepath.Join(t.TempDir(), "bad.tar")
	os.WriteFile(badPath, []byte("not a tar"), 0644)
	if err := extractTar(badPath, dst); err == nil {
		t.Fatal("expected error for invalid tar")
	}
}

func TestExtractTarEmpty(t *testing.T) {
	dst := t.TempDir()
	tarPath := makeTestTar(t, map[string]string{})
	if err := extractTar(tarPath, dst); err != nil {
		t.Fatal(err)
	}
}

// ===== copyDir =====

func TestCopyDirBasic(t *testing.T) {
	src := t.TempDir()
	os.WriteFile(filepath.Join(src, "a.txt"), []byte("aaa"), 0644)
	os.MkdirAll(filepath.Join(src, "sub"), 0755)
	os.WriteFile(filepath.Join(src, "sub", "b.txt"), []byte("bbb"), 0644)

	dst := t.TempDir()
	if err := copyDir(src, dst); err != nil {
		t.Fatal(err)
	}
	dataA, _ := os.ReadFile(filepath.Join(dst, "a.txt"))
	if string(dataA) != "aaa" {
		t.Fatalf("expected 'aaa', got %q", string(dataA))
	}
	dataB, _ := os.ReadFile(filepath.Join(dst, "sub", "b.txt"))
	if string(dataB) != "bbb" {
		t.Fatalf("expected 'bbb', got %q", string(dataB))
	}
}

func TestCopyDirNonExistentSrc(t *testing.T) {
	dst := t.TempDir()
	if err := copyDir("/nonexistent/path", dst); err == nil {
		t.Fatal("expected error for non-existent src")
	}
}

// ===== Manager Tests =====

func newTestManager(t *testing.T) *Manager {
	t.Helper()
	dir := t.TempDir()
	return NewManager(
		filepath.Join(dir, "tasks"),
		filepath.Join(dir, "task_meta"),
		filepath.Join(dir, "pipelines"),
	)
}

func writeMetaFile(t *testing.T, mgr *Manager, meta *Meta) {
	t.Helper()
	data, err := json.Marshal(meta)
	if err != nil {
		t.Fatal(err)
	}
	os.WriteFile(mgr.metaPath(meta.Name), data, 0644)
}

func TestExists(t *testing.T) {
	mgr := newTestManager(t)
	if mgr.Exists("no-such") {
		t.Fatal("expected false for non-existent task")
	}
	writeMetaFile(t, mgr, &Meta{Name: "my-task"})
	if !mgr.Exists("my-task") {
		t.Fatal("expected true for existing task")
	}
}

func TestGet(t *testing.T) {
	mgr := newTestManager(t)
	writeMetaFile(t, mgr, &Meta{Name: "get-task", RunCommand: "./run.sh"})

	meta, err := mgr.Get("get-task")
	if err != nil {
		t.Fatal(err)
	}
	if meta.Name != "get-task" || meta.RunCommand != "./run.sh" {
		t.Fatalf("got %+v", meta)
	}

	_, err = mgr.Get("no-such")
	if err == nil {
		t.Fatal("expected error for non-existent task")
	}
}

func TestAll(t *testing.T) {
	mgr := newTestManager(t)
	all, err := mgr.All()
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 0 {
		t.Fatalf("expected 0 tasks, got %d", len(all))
	}

	writeMetaFile(t, mgr, &Meta{Name: "task-a"})
	writeMetaFile(t, mgr, &Meta{Name: "task-b"})

	all, err = mgr.All()
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 2 {
		t.Fatalf("expected 2 tasks, got %d", len(all))
	}
}

func TestAllWithCorruptMeta(t *testing.T) {
	mgr := newTestManager(t)
	writeMetaFile(t, mgr, &Meta{Name: "good"})
	os.WriteFile(mgr.metaPath("bad"), []byte("{corrupt"), 0644)

	all, err := mgr.All()
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 1 {
		t.Fatalf("expected 1 task (corrupt skipped), got %d", len(all))
	}
}

func TestSetConfig(t *testing.T) {
	mgr := newTestManager(t)
	writeMetaFile(t, mgr, &Meta{Name: "cfg", RunCommand: "./old.sh"})

	if err := mgr.SetConfig("cfg", "./new.sh", "", true, 30, "skip", true, 2); err != nil {
		t.Fatal(err)
	}

	meta, _ := mgr.Get("cfg")
	if meta.RunCommand != "./new.sh" {
		t.Fatalf("expected ./new.sh, got %q", meta.RunCommand)
	}
	if !meta.TimeoutEnabled || meta.TimeoutSeconds != 30 {
		t.Fatalf("expected timeout 30 enabled, got enabled=%v sec=%d", meta.TimeoutEnabled, meta.TimeoutSeconds)
	}
	if meta.OnTimeout != "skip" {
		t.Fatalf("expected on_timeout=skip, got %q", meta.OnTimeout)
	}
	if !meta.ContinueOnFailure {
		t.Fatal("expected continue_on_failure=true")
	}
	if meta.RetryCount != 2 {
		t.Fatalf("expected retry_count=2, got %d", meta.RetryCount)
	}
}

func TestSetConfigNonExistent(t *testing.T) {
	mgr := newTestManager(t)
	err := mgr.SetConfig("no-such", "x", "", false, 0, "", false, 0)
	if err == nil {
		t.Fatal("expected error for non-existent task")
	}
}

func TestSetConfigInvalidOnTimeout(t *testing.T) {
	mgr := newTestManager(t)
	writeMetaFile(t, mgr, &Meta{Name: "bad"})
	err := mgr.SetConfig("bad", "x", "", false, 0, "garbage", false, 0)
	if err == nil {
		t.Fatal("expected error for invalid on_timeout")
	}
}

func TestDelete(t *testing.T) {
	mgr := newTestManager(t)
	os.MkdirAll(filepath.Join(mgr.tasksDir, "del-task"), 0755)
	writeMetaFile(t, mgr, &Meta{Name: "del-task", PackagePath: "tasks/del-task"})

	if err := mgr.Delete("del-task"); err != nil {
		t.Fatal(err)
	}
	if mgr.Exists("del-task") {
		t.Fatal("task should not exist after delete")
	}
	if _, err := os.Stat(filepath.Join(mgr.tasksDir, "del-task")); !os.IsNotExist(err) {
		t.Fatal("task dir should be removed after delete")
	}
}

func TestDeleteNonExistent(t *testing.T) {
	mgr := newTestManager(t)
	err := mgr.Delete("no-such")
	if err == nil {
		t.Fatal("expected error for non-existent task")
	}
}

func TestDeleteTaskReferencedByPipeline(t *testing.T) {
	mgr := newTestManager(t)
	os.MkdirAll(filepath.Join(mgr.tasksDir, "used"), 0755)
	writeMetaFile(t, mgr, &Meta{Name: "used", PackagePath: "tasks/used"})

	// Create a pipeline file referencing this task
	os.MkdirAll(mgr.pipelinesDir, 0755)
	pipeData := map[string]interface{}{
		"id":   "pipeline-1",
		"name": "test",
		"tasks": []map[string]interface{}{
			{"name": "used"},
		},
	}
	f, _ := os.Create(filepath.Join(mgr.pipelinesDir, "pipeline-1.json"))
	json.NewEncoder(f).Encode(pipeData)
	f.Close()

	err := mgr.Delete("used")
	if err == nil || !strings.Contains(err.Error(), "used by") {
		t.Fatalf("expected 'used by' error, got: %v", err)
	}
}

func TestPipelines(t *testing.T) {
	mgr := newTestManager(t)
	os.MkdirAll(mgr.pipelinesDir, 0755)

	// Write pipeline referencing "my-task"
	pipeData := map[string]interface{}{
		"id":   "pipeline-1",
		"name": "test",
		"tasks": []map[string]interface{}{
			{"name": "my-task"},
		},
	}
	f, _ := os.Create(filepath.Join(mgr.pipelinesDir, "pipeline-1.json"))
	json.NewEncoder(f).Encode(pipeData)
	f.Close()

	ids, err := mgr.Pipelines("my-task")
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 1 || ids[0] != "pipeline-1" {
		t.Fatalf("expected [pipeline-1], got %v", ids)
	}

	// Non-referenced task
	ids, err = mgr.Pipelines("other")
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 0 {
		t.Fatalf("expected 0 pipelines, got %v", ids)
	}
}

func TestPipelinesNonExistentDir(t *testing.T) {
	mgr := &Manager{pipelinesDir: "/nonexistent"}
	ids, err := mgr.Pipelines("x")
	if err != nil {
		t.Fatal(err)
	}
	if ids != nil {
		t.Fatalf("expected nil, got %v", ids)
	}
}

func TestParseReadmeMethod(t *testing.T) {
	mgr := newTestManager(t)
	taskDir := filepath.Join(mgr.tasksDir, "readme-task")
	os.MkdirAll(taskDir, 0755)
	os.WriteFile(filepath.Join(taskDir, "README.md"), []byte("content"), 0644)

	content, found := mgr.ParseReadme("readme-task")
	if !found || content != "content" {
		t.Fatalf("expected found=true content='content', got found=%v content=%q", found, content)
	}

	_, found = mgr.ParseReadme("no-such")
	if found {
		t.Fatal("expected not found for non-existent task")
	}
}

func TestExport(t *testing.T) {
	mgr := newTestManager(t)
	taskDir := filepath.Join(mgr.tasksDir, "export-me")
	os.MkdirAll(taskDir, 0755)
	os.WriteFile(filepath.Join(taskDir, "run.sh"), []byte("echo hi"), 0755)
	writeMetaFile(t, mgr, &Meta{Name: "export-me", PackagePath: "tasks/export-me"})

	tarPath, err := mgr.Export("export-me")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tarPath)

	if !strings.HasSuffix(tarPath, ".tar") {
		t.Fatalf("expected .tar file, got %s", tarPath)
	}

	// Verify it's a valid tar
	f, err := os.Open(tarPath)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	tr := tar.NewReader(f)
	found := false
	for {
		hdr, err := tr.Next()
		if err != nil {
			break
		}
		if hdr.Name == "run.sh" {
			found = true
		}
	}
	if !found {
		t.Fatal("expected run.sh in tar")
	}
}

func TestExportNonExistent(t *testing.T) {
	mgr := newTestManager(t)
	_, err := mgr.Export("no-such")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestExportMissingDir(t *testing.T) {
	mgr := newTestManager(t)
	writeMetaFile(t, mgr, &Meta{Name: "missing-dir", PackagePath: "tasks/missing-dir"})
	_, err := mgr.Export("missing-dir")
	if err == nil {
		t.Fatal("expected error for missing package dir")
	}
}

func TestUploadInvalidTarName(t *testing.T) {
	mgr := newTestManager(t)
	_, err := mgr.Upload("/path/to/noext")
	if err == nil || !strings.Contains(err.Error(), "invalid tar filename") {
		t.Fatalf("expected invalid tar filename error, got %v", err)
	}
}

func TestUploadInvalidTaskName(t *testing.T) {
	mgr := newTestManager(t)
	path := filepath.Join(t.TempDir(), "@invalid!.tar")
	os.WriteFile(path, []byte("content"), 0644)
	_, err := mgr.Upload(path)
	if err == nil || !strings.Contains(err.Error(), "invalid task name") {
		t.Fatalf("expected invalid task name error, got %v", err)
	}
}

func TestUploadDuplicateTask(t *testing.T) {
	mgr := newTestManager(t)
	writeMetaFile(t, mgr, &Meta{Name: "dup"})

	path := filepath.Join(t.TempDir(), "dup.tar")
	os.WriteFile(path, []byte("content"), 0644)
	_, err := mgr.Upload(path)
	if err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("expected already exists error, got %v", err)
	}
}
