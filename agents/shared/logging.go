package shared

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
)

type LogLevel int

const (
	INFO LogLevel = iota
	WARN
	ERROR
)

func (l LogLevel) String() string {
	switch l {
	case INFO:
		return "INFO"
	case WARN:
		return "WARN"
	case ERROR:
		return "ERROR"
	default:
		return "UNKNOWN"
	}
}

type AgentLogger struct {
	agentName string
	logger    *log.Logger
	file      *os.File
	mu        sync.Mutex
}

func NewAgentLogger(agentName string, logDir string) (*AgentLogger, error) {
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return nil, fmt.Errorf("create log dir: %w", err)
	}

	logFile := filepath.Join(logDir, agentName+".log")
	f, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("open log file: %w", err)
	}

	multi := io.MultiWriter(os.Stdout, f)
	prefix := fmt.Sprintf("[%s] ", agentName)
	logger := log.New(multi, prefix, log.Ldate|log.Ltime)

	return &AgentLogger{
		agentName: agentName,
		logger:    logger,
		file:      f,
	}, nil
}

func (l *AgentLogger) logf(level LogLevel, format string, args ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	msg := fmt.Sprintf(format, args...)
	l.logger.Printf("%-5s %s", level.String(), msg)
}

func (l *AgentLogger) Info(format string, args ...interface{}) {
	l.logf(INFO, format, args...)
}

func (l *AgentLogger) Warn(format string, args ...interface{}) {
	l.logf(WARN, format, args...)
}

func (l *AgentLogger) Error(format string, args ...interface{}) {
	l.logf(ERROR, format, args...)
}

func (l *AgentLogger) Close() {
	if l.file != nil {
		l.file.Close()
	}
}
