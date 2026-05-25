package runner

import (
	"strconv"
	"strings"
	"time"
)

// ValidCron checks whether a cron expression is syntactically valid.
func ValidCron(expr string) bool {
	fields := strings.Fields(expr)
	if len(fields) != 5 {
		return false
	}
	bounds := [][2]int{{0, 59}, {0, 23}, {1, 31}, {1, 12}, {0, 6}}
	for i, f := range fields {
		if !validField(f, bounds[i][0], bounds[i][1]) {
			return false
		}
	}
	return true
}

func validField(expr string, min, max int) bool {
	if strings.Contains(expr, ",") {
		for _, part := range strings.Split(expr, ",") {
			if !validField(part, min, max) {
				return false
			}
		}
		return true
	}
	if strings.Contains(expr, "/") {
		parts := strings.SplitN(expr, "/", 2)
		if len(parts) != 2 {
			return false
		}
		step, err := strconv.Atoi(parts[1])
		if err != nil || step <= 0 {
			return false
		}
		return validField(parts[0], min, max)
	}
	if strings.Contains(expr, "-") {
		parts := strings.SplitN(expr, "-", 2)
		start, err1 := strconv.Atoi(parts[0])
		end, err2 := strconv.Atoi(parts[1])
		if err1 != nil || err2 != nil || start < min || end > max || start > end {
			return false
		}
		return true
	}
	if expr == "*" {
		return true
	}
	n, err := strconv.Atoi(expr)
	if err != nil || n < min || n > max {
		return false
	}
	return true
}

func MatchCron(expr string, t time.Time) bool {
	fields := strings.Fields(expr)
	if len(fields) != 5 {
		return false
	}
	matches := []bool{
		fieldMatches(fields[0], t.Minute(), 0, 59),
		fieldMatches(fields[1], t.Hour(), 0, 23),
		fieldMatches(fields[2], t.Day(), 1, 31),
		fieldMatches(fields[3], int(t.Month()), 1, 12),
		fieldMatches(fields[4], int(t.Weekday()), 0, 6),
	}
	for _, m := range matches {
		if !m {
			return false
		}
	}
	return true
}

func fieldMatches(expr string, value, min, max int) bool {
	if strings.Contains(expr, ",") {
		for _, part := range strings.Split(expr, ",") {
			if fieldMatches(part, value, min, max) {
				return true
			}
		}
		return false
	}

	if strings.Contains(expr, "/") {
		parts := strings.SplitN(expr, "/", 2)
		if len(parts) != 2 {
			return false
		}
		step, err := strconv.Atoi(parts[1])
		if err != nil || step <= 0 {
			return false
		}
		if parts[0] == "*" {
			return (value-min)%step == 0
		}
		if strings.Contains(parts[0], "-") {
			rangeParts := strings.SplitN(parts[0], "-", 2)
			start, err1 := strconv.Atoi(rangeParts[0])
			end, err2 := strconv.Atoi(rangeParts[1])
			if err1 != nil || err2 != nil || start < min || end > max || start > end {
				return false
			}
			if value < start || value > end {
				return false
			}
			return (value-start)%step == 0
		}
		n, err := strconv.Atoi(parts[0])
		if err != nil {
			return false
		}
		if value < n {
			return false
		}
		return (value-n)%step == 0
	}

	if strings.Contains(expr, "-") {
		parts := strings.SplitN(expr, "-", 2)
		start, err1 := strconv.Atoi(parts[0])
		end, err2 := strconv.Atoi(parts[1])
		if err1 != nil || err2 != nil || start < min || end > max || start > end {
			return false
		}
		return value >= start && value <= end
	}

	if expr == "*" {
		return true
	}

	n, err := strconv.Atoi(expr)
	if err != nil {
		return false
	}
	return n == value
}
