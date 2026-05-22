package shared

import (
	"fmt"

	"sync/atomic"
	"time"
)

type Metrics struct {
	agentName       string
	TasksReceived   atomic.Int64 `json:"tasks_received"`
	TasksSucceeded  atomic.Int64 `json:"tasks_succeeded"`
	TasksFailed     atomic.Int64 `json:"tasks_failed"`
	ProcessingTimeMs atomic.Int64 `json:"processing_time_ms"`

	startedAt       time.Time
	logger          *AgentLogger
}

func NewMetrics(agentName string, logger *AgentLogger) *Metrics {
	return &Metrics{
		agentName: agentName,
		startedAt: time.Now(),
		logger:    logger,
	}
}

func (m *Metrics) IncReceived() {
	m.TasksReceived.Add(1)
}

func (m *Metrics) IncSucceeded() {
	m.TasksSucceeded.Add(1)
}

func (m *Metrics) IncFailed() {
	m.TasksFailed.Add(1)
}

func (m *Metrics) AddProcessingTime(ms int64) {
	m.ProcessingTimeMs.Add(ms)
}

func (m *Metrics) LogAndReport() {
	received := m.TasksReceived.Load()
	succeeded := m.TasksSucceeded.Load()
	failed := m.TasksFailed.Load()
	totalTime := m.ProcessingTimeMs.Load()
	uptime := time.Since(m.startedAt).Round(time.Second)

	var avgMs int64
	if succeeded > 0 {
		avgMs = totalTime / succeeded
	}



	m.logger.Info("=== МЕТРИКИ === получено=%d успешно=%d ошибок=%d среднее_время=%dms uptime=%s",
		received, succeeded, failed, avgMs, uptime)
}

func (m *Metrics) Report() string {
	received := m.TasksReceived.Load()
	succeeded := m.TasksSucceeded.Load()
	failed := m.TasksFailed.Load()
	totalTime := m.ProcessingTimeMs.Load()
	uptime := time.Since(m.startedAt).Round(time.Second)

	var avgMs int64
	if succeeded > 0 {
		avgMs = totalTime / succeeded
	}

	return fmt.Sprintf(
		`{"agent":"%s","uptime":"%s","tasks":{"received":%d,"succeeded":%d,"failed":%d,"avg_time_ms":%d}}`,
		m.agentName, uptime, received, succeeded, failed, avgMs,
	)
}
