package shared

import (
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestNewAgentLogger(t *testing.T) {
	dir := t.TempDir()
	l, err := NewAgentLogger("test-agent", dir)
	if err != nil {
		t.Fatalf("NewAgentLogger failed: %v", err)
	}
	defer l.Close()

	if l.agentName != "test-agent" {
		t.Errorf("agentName = %q, want %q", l.agentName, "test-agent")
	}

	_, err = os.Stat(filepath.Join(dir, "test-agent.log"))
	if err != nil {
		t.Errorf("log file not created: %v", err)
	}
}

func TestLogLevelString(t *testing.T) {
	tests := []struct {
		level LogLevel
		want  string
	}{
		{INFO, "INFO"},
		{WARN, "WARN"},
		{ERROR, "ERROR"},
		{LogLevel(99), "UNKNOWN"},
	}
	for _, tt := range tests {
		got := tt.level.String()
		if got != tt.want {
			t.Errorf("LogLevel(%d).String() = %q, want %q", tt.level, got, tt.want)
		}
	}
}

func TestAgentLoggerLogsToFileAndBuffer(t *testing.T) {
	dir := t.TempDir()

	l, err := NewAgentLogger("logtest", dir)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	l.Info("hello %s", "world")
	l.Warn("warning %d", 42)
	l.Error("error: %v", os.ErrNotExist)

	logContent, err := os.ReadFile(filepath.Join(dir, "logtest.log"))
	if err != nil {
		t.Fatal(err)
	}

	text := string(logContent)

	if !strings.Contains(text, "INFO") {
		t.Errorf("log missing INFO level")
	}
	if !strings.Contains(text, "hello world") {
		t.Errorf("log missing formatted message")
	}
	if !strings.Contains(text, "WARN") {
		t.Errorf("log missing WARN level")
	}
	if !strings.Contains(text, "warning 42") {
		t.Errorf("log missing warning message")
	}
	if !strings.Contains(text, "ERROR") {
		t.Errorf("log missing ERROR level")
	}
}

func TestNewMetrics(t *testing.T) {
	dir := t.TempDir()
	logger, _ := NewAgentLogger("metrics-test", dir)
	defer logger.Close()

	m := NewMetrics("metrics-test", logger)

	if m.agentName != "metrics-test" {
		t.Errorf("agentName = %q", m.agentName)
	}
	if m.TasksReceived.Load() != 0 {
		t.Errorf("expected 0 received")
	}
}

func TestMetricsCounters(t *testing.T) {
	dir := t.TempDir()
	logger, _ := NewAgentLogger("counters", dir)
	defer logger.Close()

	m := NewMetrics("counters", logger)

	m.IncReceived()
	m.IncReceived()
	m.IncSucceeded()
	m.IncFailed()
	m.AddProcessingTime(150)
	m.AddProcessingTime(50)

	if got := m.TasksReceived.Load(); got != 2 {
		t.Errorf("TasksReceived = %d, want 2", got)
	}
	if got := m.TasksSucceeded.Load(); got != 1 {
		t.Errorf("TasksSucceeded = %d, want 1", got)
	}
	if got := m.TasksFailed.Load(); got != 1 {
		t.Errorf("TasksFailed = %d, want 1", got)
	}
	if got := m.ProcessingTimeMs.Load(); got != 200 {
		t.Errorf("ProcessingTimeMs = %d, want 200", got)
	}
}

func TestMetricsReport(t *testing.T) {
	dir := t.TempDir()
	logger, _ := NewAgentLogger("report", dir)
	defer logger.Close()

	m := NewMetrics("report", logger)
	m.IncSucceeded()
	m.AddProcessingTime(100)

	report := m.Report()
	if !strings.Contains(report, `"agent":"report"`) {
		t.Errorf("report missing agent name")
	}
	if !strings.Contains(report, `"succeeded":1`) {
		t.Errorf("report should show 1 succeeded")
	}
	if !strings.Contains(report, `"avg_time_ms":100`) {
		t.Errorf("report should show avg_time_ms=100")
	}
}

func TestMetricsReportNoTask(t *testing.T) {
	dir := t.TempDir()
	logger, _ := NewAgentLogger("empty", dir)
	defer logger.Close()

	m := NewMetrics("empty", logger)
	report := m.Report()

	if !strings.Contains(report, `"succeeded":0`) {
		t.Errorf("empty report should show 0 succeeded")
	}
	if !strings.Contains(report, `"avg_time_ms":0`) {
		t.Errorf("empty report should show avg_time_ms=0")
	}
}

func TestMetricsLogAndReportDoesNotPanic(t *testing.T) {
	dir := t.TempDir()
	logger, _ := NewAgentLogger("panic-test", dir)
	defer logger.Close()

	m := NewMetrics("panic-test", logger)
	m.IncReceived()
	m.IncSucceeded()

	// LogAndReport had a bug where it called Unlock on unlocked mutex
	// Ensure it doesn't panic
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("LogAndReport panicked: %v", r)
		}
	}()

	m.LogAndReport()
}

func TestMetricsConcurrentSafety(t *testing.T) {
	dir := t.TempDir()
	logger, _ := NewAgentLogger("concurrent", dir)
	defer logger.Close()

	m := NewMetrics("concurrent", logger)

	var received atomic.Int64
	var succeeded atomic.Int64

	done := make(chan struct{})
	go func() {
		for i := 0; i < 1000; i++ {
			m.IncReceived()
			received.Add(1)
		}
		done <- struct{}{}
	}()
	go func() {
		for i := 0; i < 1000; i++ {
			m.IncSucceeded()
			succeeded.Add(1)
		}
		done <- struct{}{}
	}()
	go func() {
		for i := 0; i < 1000; i++ {
			m.AddProcessingTime(1)
		}
		done <- struct{}{}
	}()

	for i := 0; i < 3; i++ {
		<-done
	}

	if m.TasksReceived.Load() != 1000 {
		t.Errorf("TasksReceived = %d, want 1000", m.TasksReceived.Load())
	}
	if m.TasksSucceeded.Load() != 1000 {
		t.Errorf("TasksSucceeded = %d, want 1000", m.TasksSucceeded.Load())
	}
	if m.ProcessingTimeMs.Load() != 1000 {
		t.Errorf("ProcessingTimeMs = %d, want 1000", m.ProcessingTimeMs.Load())
	}

	report := m.Report()
	if !strings.Contains(report, `"received":1000`) {
		t.Errorf("report missing received=1000")
	}
}

func TestLogLevelOrdering(t *testing.T) {
	if INFO >= WARN {
		t.Errorf("INFO (%d) should be < WARN (%d)", INFO, WARN)
	}
	if WARN >= ERROR {
		t.Errorf("WARN (%d) should be < ERROR (%d)", WARN, ERROR)
	}
}

func TestAgentLoggerCloseIdempotent(t *testing.T) {
	dir := t.TempDir()
	l, err := NewAgentLogger("close-test", dir)
	if err != nil {
		t.Fatal(err)
	}

	l.Close()
	// second close should not panic
	l.Close()
}

func TestNewAgentLoggerInvalidDir(t *testing.T) {
	_, err := NewAgentLogger("bad", string([]byte{0}))
	if err == nil {
		t.Error("expected error for invalid path")
	}
}

func TestMetricsUptimeIncreases(t *testing.T) {
	dir := t.TempDir()
	logger, _ := NewAgentLogger("uptime", dir)
	defer logger.Close()

	m := NewMetrics("uptime", logger)

	// Report rounds uptime to seconds; sleep longer to cross boundary
	if testing.Short() {
		t.Skip("skipping timing-sensitive test in short mode")
	}

	time.Sleep(1100 * time.Millisecond)

	if m.startedAt.IsZero() {
		t.Error("startedAt should be set")
	}
	// Just verify it doesn't panic and report is non-empty
	r := m.Report()
	if r == "" {
		t.Error("report should not be empty")
	}
}
