package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"time"

	"lab5/agents/shared"

	"github.com/nats-io/nats.go"
)

type ScheduleItem struct {
	Machine  string `json:"machine"`
	Task     string `json:"task"`
	Duration int    `json:"duration_mins"`
}

type DispatchRequest struct {
	OrderID    string         `json:"order_id"`
	Schedule   []ScheduleItem `json:"schedule"`
	Priority   string         `json:"priority"`
	StartAfter string         `json:"start_after"`
}

type LineStatus struct {
	Line        string `json:"line"`
	Task        string `json:"task"`
	Status      string `json:"status"`
	ActualStart string `json:"actual_start,omitempty"`
	ExpectedEnd string `json:"expected_end,omitempty"`
}

type DispatchResult struct {
	OrderID       string       `json:"order_id"`
	DispatchID    string       `json:"dispatch_id"`
	Lines         []LineStatus `json:"lines"`
	OverallStatus string       `json:"overall_status"`
}

type ProductionLine struct {
	Name  string
	Busy  bool
	Queue []string
}

var (
	lines       []*ProductionLine
	linesLock   sync.Mutex
	dispatchCnt int
)

func init() {
	lines = []*ProductionLine{
		{Name: "Line-A", Busy: false},
		{Name: "Line-B", Busy: false},
		{Name: "Line-C", Busy: false},
		{Name: "Line-D", Busy: false},
	}
}

func getLogDir() string {
	if d := os.Getenv("AGENT_LOG_DIR"); d != "" {
		return d
	}
	return "logs"
}

func main() {
	logDir := getLogDir()
	logger, err := shared.NewAgentLogger("dispatcher", logDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "logger init: %v\n", err)
		os.Exit(1)
	}
	defer logger.Close()

	metrics := shared.NewMetrics("dispatcher", logger)
	logger.Info("Агент запущен, ожидание задач...")

	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		logger.Error("Ошибка подключения к NATS: %v", err)
		os.Exit(1)
	}
	defer nc.Close()

	nc.Subscribe("production.dispatch", func(m *nats.Msg) {
		start := time.Now()
		metrics.IncReceived()

		var task shared.Task
		if err := json.Unmarshal(m.Data, &task); err != nil {
			logger.Error("Ошибка разбора задачи: %v", err)
			metrics.IncFailed()
			return
		}

		logger.Info("Получена задача %s типа %s", task.ID, task.Type)

		var req DispatchRequest
		if err := json.Unmarshal([]byte(task.Payload), &req); err != nil {
			logger.Error("Ошибка разбора payload задачи %s: %v", task.ID, err)
			metrics.IncFailed()
			return
		}

		logger.Info("Обработка: order=%s priority=%s schedule=%d задач",
			req.OrderID, req.Priority, len(req.Schedule))

		result := dispatch(req)
		output, _ := json.Marshal(result)
		logger.Info("Задача %s выполнена: dispatch=%s status=%s",
			task.ID, result.DispatchID, result.OverallStatus)

		res := shared.Result{
			TaskID:  task.ID,
			Success: true,
			Output:  string(output),
			Agent:   "dispatcher",
		}
		response, _ := json.Marshal(res)
		nc.Publish("production.completed", response)

		metrics.IncSucceeded()
		metrics.AddProcessingTime(time.Since(start).Milliseconds())
	})

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		<-c
		logger.Info("Завершение работы...")
		metrics.LogAndReport()
		os.Exit(0)
	}()

	select {}
}

func dispatch(req DispatchRequest) DispatchResult {
	linesLock.Lock()
	defer linesLock.Unlock()

	dispatchCnt++
	dispatchID := "DSP-" + time.Now().Format("20060102") + "-" + itoa(dispatchCnt)

	result := DispatchResult{
		OrderID:    req.OrderID,
		DispatchID: dispatchID,
	}

	startTime := time.Now()
	if req.StartAfter != "" {
		if parsed, err := time.Parse("15:04 02.01.2006", req.StartAfter); err == nil {
			if parsed.After(startTime) {
				startTime = parsed
			}
		}
	}

	isCritical := req.Priority == "critical" || req.Priority == "high"

	if len(req.Schedule) == 0 {
		req.Schedule = generateDefaultSchedule(req.OrderID)
	}

	for _, s := range req.Schedule {
		line := selectLine(s.Machine, isCritical)
		assignedTime := startTime

		if line.Busy {
			if isCritical {
				preempted := line.Queue
				line.Queue = []string{req.OrderID}
				line.Queue = append(line.Queue, preempted...)
				assignedTime = assignedTime.Add(5 * time.Minute)

				ls := LineStatus{
					Line:   line.Name,
					Task:   s.Task + " (прерывание)",
					Status: "preempted",
				}
				result.Lines = append(result.Lines, ls)
			} else {
				line.Queue = append(line.Queue, s.Task)
				estStart := assignedTime.Add(time.Duration(len(line.Queue)*15) * time.Minute)
				ls := LineStatus{
					Line:        line.Name,
					Task:        s.Task,
					Status:      "queued",
					ActualStart: estStart.Format("15:04 02.01.2006"),
				}
				result.Lines = append(result.Lines, ls)
				continue
			}
		}

		line.Busy = true
		endTime := assignedTime.Add(time.Duration(s.Duration) * time.Minute)

		setupDelay := time.Duration(rand.Intn(10)) * time.Minute
		actualStart := assignedTime.Add(setupDelay)

		ls := LineStatus{
			Line:        line.Name,
			Task:        s.Task,
			Status:      "in_progress",
			ActualStart: actualStart.Format("15:04 02.01.2006"),
			ExpectedEnd: endTime.Format("15:04 02.01.2006"),
		}
		result.Lines = append(result.Lines, ls)

		go func(l *ProductionLine, dur time.Duration) {
			time.Sleep(dur)
			linesLock.Lock()
			l.Busy = false
			if len(l.Queue) > 0 {
				l.Queue = l.Queue[1:]
			}
			linesLock.Unlock()
		}(line, time.Duration(s.Duration)*time.Minute)
	}

	allInProgress := true
	allQueued := true
	for _, l := range result.Lines {
		if l.Status != "queued" {
			allQueued = false
		}
		if l.Status != "in_progress" && l.Status != "preempted" {
			allInProgress = false
		}
	}

	switch {
	case allQueued:
		result.OverallStatus = "queued"
	case allInProgress:
		result.OverallStatus = "in_progress"
	default:
		result.OverallStatus = "partially_dispatched"
	}

	return result
}

func selectLine(preferredMachine string, isCritical bool) *ProductionLine {
	for _, l := range lines {
		if stringsEqualFold(l.Name, preferredMachine) {
			return l
		}
	}

	bestLine := lines[0]
	minQueue := len(lines[0].Queue)
	for _, l := range lines[1:] {
		load := len(l.Queue)
		if !l.Busy {
			return l
		}
		if load < minQueue {
			minQueue = load
			bestLine = l
		}
	}
	return bestLine
}

func stringsEqualFold(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := 0; i < len(a); i++ {
		ca := a[i]
		cb := b[i]
		if ca >= 'A' && ca <= 'Z' {
			ca += 32
		}
		if cb >= 'A' && cb <= 'Z' {
			cb += 32
		}
		if ca != cb {
			return false
		}
	}
	return true
}

func generateDefaultSchedule(orderID string) []ScheduleItem {
	return []ScheduleItem{
		{Machine: "Line-A", Task: "Обработка заказа " + orderID, Duration: 120},
		{Machine: "Line-B", Task: "Сборка заказа " + orderID, Duration: 90},
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	result := ""
	for n > 0 {
		digit := n % 10
		result = string(rune('0'+digit)) + result
		n /= 10
	}
	return result
}
