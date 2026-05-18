package logger

import (
	"compress/gzip"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func New(level, logPath string) (*slog.Logger, error) {
	var l slog.Level
	switch strings.ToLower(level) {
	case "debug":
		l = slog.LevelDebug
	case "info":
		l = slog.LevelInfo
	case "warn":
		l = slog.LevelWarn
	case "error":
		l = slog.LevelError
	default:
		return nil, fmt.Errorf("unknown log level: %s (valid: debug, info, warn, error)", level)
	}

	f, err := os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("open log file: %w", err)
	}

	w := io.MultiWriter(f, os.Stderr)
	h := slog.NewTextHandler(w, &slog.HandlerOptions{Level: l})
	return slog.New(h), nil
}

func Rotate(logDir, pattern string) (compressed, deleted int) {
	matches, err := filepath.Glob(filepath.Join(logDir, pattern))
	if err != nil {
		return
	}

	now := time.Now()
	week := 7 * 24 * time.Hour
	year := 365 * 24 * time.Hour

	for _, p := range matches {
		fi, err := os.Stat(p)
		if err != nil {
			continue
		}
		age := now.Sub(fi.ModTime())

		if strings.HasSuffix(p, ".gz") {
			if age > year {
				if os.Remove(p) == nil {
					deleted++
				}
			}
			continue
		}

		if age > week {
			gzPath := p + ".gz"
			if err := gzipFile(p, gzPath); err == nil {
				os.Remove(p)
				compressed++
			}
		}
	}
	return
}

func gzipFile(src, dst string) (err error) {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := out.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}()

	gw := gzip.NewWriter(out)
	defer func() {
		if cerr := gw.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}()

	_, err = io.Copy(gw, in)
	return err
}
