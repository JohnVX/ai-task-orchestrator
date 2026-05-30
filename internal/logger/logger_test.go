package logger

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestNewValidLevels(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	for _, level := range []string{"debug", "info", "warn", "error"} {
		logPath := filepath.Join(dir, level+".log")
		l, err := New(level, logPath)
		if err != nil {
			t.Fatalf("New(%q): %v", level, err)
		}
		if l == nil {
			t.Fatalf("New(%q) returned nil logger", level)
		}
		if _, err := os.Stat(logPath); err != nil {
			t.Fatalf("log file %s not created: %v", logPath, err)
		}
	}
}

func TestNewInvalidLevel(t *testing.T) {
	t.Parallel()
	_, err := New("invalid", filepath.Join(t.TempDir(), "test.log"))
	if err == nil {
		t.Fatal("expected error for invalid level")
	}
	if !strings.Contains(err.Error(), "unknown log level") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNewAppendsToExistingFile(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	logPath := filepath.Join(dir, "append.log")
	if err := os.WriteFile(logPath, []byte("preexisting\n"), 0644); err != nil {
		t.Fatal(err)
	}
	l, err := New("info", logPath)
	if err != nil {
		t.Fatal(err)
	}
	if l == nil {
		t.Fatal("expected non-nil logger")
	}
	l.Info("second line")
	data, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), "second line") {
		t.Fatal("expected new log line to be appended")
	}
}

func TestRotateNoMatches(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	c, d := Rotate(dir, "nonexistent*.log")
	if c != 0 || d != 0 {
		t.Fatalf("expected 0,0 got %d,%d", c, d)
	}
}

func TestRotateCompressesOldLog(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "orchestrator.log")

	// Create old log file (mod time > 1 week ago)
	if err := os.WriteFile(logPath, []byte("test log content\n"), 0644); err != nil {
		t.Fatal(err)
	}
	oldTime := time.Now().Add(-8 * 24 * time.Hour)
	if err := os.Chtimes(logPath, oldTime, oldTime); err != nil {
		t.Fatal(err)
	}

	c, d := Rotate(dir, "orchestrator.log*")
	if c != 1 {
		t.Fatalf("expected 1 compressed, got %d", c)
	}
	if d != 0 {
		t.Fatalf("expected 0 deleted, got %d", d)
	}

	// Original should be gone, .gz should exist
	if _, err := os.Stat(logPath); !os.IsNotExist(err) {
		t.Fatal("original log file should have been removed")
	}
	if _, err := os.Stat(logPath + ".gz"); err != nil {
		t.Fatalf("compressed file not found: %v", err)
	}
}

func TestRotateDeletesOldCompressed(t *testing.T) {
	dir := t.TempDir()
	gzPath := filepath.Join(dir, "orchestrator.log.gz")

	if err := os.WriteFile(gzPath, []byte("old compressed"), 0644); err != nil {
		t.Fatal(err)
	}
	oldTime := time.Now().Add(-400 * 24 * time.Hour) // > 1 year
	if err := os.Chtimes(gzPath, oldTime, oldTime); err != nil {
		t.Fatal(err)
	}

	c, d := Rotate(dir, "orchestrator.log*")
	if c != 0 {
		t.Fatalf("expected 0 compressed, got %d", c)
	}
	if d != 1 {
		t.Fatalf("expected 1 deleted, got %d", d)
	}
	if _, err := os.Stat(gzPath); !os.IsNotExist(err) {
		t.Fatal("old .gz should have been deleted")
	}
}

func TestRotateSkipsRecentFile(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	logPath := filepath.Join(dir, "recent.log")

	if err := os.WriteFile(logPath, []byte("recent"), 0644); err != nil {
		t.Fatal(err)
	}
	// mod time = now (default)

	c, d := Rotate(dir, "recent.log*")
	if c != 0 || d != 0 {
		t.Fatalf("expected 0,0 for recent file, got %d,%d", c, d)
	}
	if _, err := os.Stat(logPath); err != nil {
		t.Fatal("recent file should not have been removed")
	}
}
