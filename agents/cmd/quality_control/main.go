package main

import (
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"time"

	"lab5/agents/shared"

	"github.com/nats-io/nats.go"
)

type Measurement struct {
	Param    string  `json:"param"`
	Value    float64 `json:"value"`
	Nominal  float64 `json:"nominal"`
	Unit     string  `json:"unit"`
	Critical bool    `json:"critical"`
}

type QCRequest struct {
	BatchID      string        `json:"batch_id"`
	Product      string        `json:"product"`
	Quantity     int           `json:"quantity"`
	Measurements []Measurement `json:"measurements"`
	OrderID      string        `json:"order_id"`
}

type QCDefect struct {
	Param     string  `json:"param"`
	Deviation float64 `json:"deviation_pct"`
	Severity  string  `json:"severity"`
}

type QCResult struct {
	BatchID    string     `json:"batch_id"`
	Status     string     `json:"status"`
	DefectRate float64    `json:"defect_rate"`
	PassedPct  float64    `json:"passed_pct"`
	Defects    []QCDefect `json:"defects,omitempty"`
	ReworkNote string     `json:"rework_note,omitempty"`
	RejectNote string     `json:"reject_note,omitempty"`
}

func getLogDir() string {
	if d := os.Getenv("AGENT_LOG_DIR"); d != "" {
		return d
	}
	return "logs"
}

func main() {
	logDir := getLogDir()
	logger, err := shared.NewAgentLogger("quality_control", logDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "logger init: %v\n", err)
		os.Exit(1)
	}
	defer logger.Close()

	metrics := shared.NewMetrics("quality_control", logger)
	logger.Info("Агент запущен, ожидание задач...")

	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		logger.Error("Ошибка подключения к NATS: %v", err)
		os.Exit(1)
	}
	defer nc.Close()

	nc.Subscribe("production.quality", func(m *nats.Msg) {
		start := time.Now()
		metrics.IncReceived()

		var task shared.Task
		if err := json.Unmarshal(m.Data, &task); err != nil {
			logger.Error("Ошибка разбора задачи: %v", err)
			metrics.IncFailed()
			return
		}

		logger.Info("Получена задача %s типа %s", task.ID, task.Type)

		var req QCRequest
		if err := json.Unmarshal([]byte(task.Payload), &req); err != nil {
			logger.Error("Ошибка разбора payload задачи %s: %v", task.ID, err)
			metrics.IncFailed()
			return
		}

		logger.Info("Обработка: batch=%s product=%s qty=%d",
			req.BatchID, req.Product, req.Quantity)

		result := inspectQuality(req)
		output, _ := json.Marshal(result)
		logger.Info("Задача %s выполнена: status=%s defect_rate=%.1f%%",
			task.ID, result.Status, result.DefectRate)

		res := shared.Result{
			TaskID:  task.ID,
			Success: true,
			Output:  string(output),
			Agent:   "quality_control",
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

func inspectQuality(req QCRequest) QCResult {
	sampleSize := int(math.Max(10, float64(req.Quantity)*0.2))

	if len(req.Measurements) == 0 {
		req.Measurements = generateSampleMeasurements(req.Product, sampleSize)
	}

	var defects []QCDefect
	defectCount := 0
	totalChecks := len(req.Measurements)

	for _, m := range req.Measurements {
		tolerance := 5.0
		if m.Critical {
			tolerance = 1.0
		}

		if m.Nominal == 0 {
			continue
		}

		deviation := math.Abs(m.Value-m.Nominal) / m.Nominal * 100

		if deviation > tolerance {
			defectCount++
			severity := "minor"
			if deviation > tolerance*2 {
				severity = "major"
			}
			if m.Critical && deviation > 1.0 {
				severity = "critical"
			}
			defects = append(defects, QCDefect{
				Param:     m.Param,
				Deviation: math.Round(deviation*100) / 100,
				Severity:  severity,
			})
		}
	}

	defectRate := float64(defectCount) / float64(totalChecks) * 100
	passedPct := 100 - defectRate

	result := QCResult{
		BatchID:    req.BatchID,
		DefectRate: math.Round(defectRate*100) / 100,
		PassedPct:  math.Round(passedPct*100) / 100,
	}

	hasCritical := false
	for _, d := range defects {
		if d.Severity == "critical" {
			hasCritical = true
			break
		}
	}

	switch {
	case defectRate > 10 || hasCritical:
		result.Status = "rejected"
		result.Defects = defects
		result.RejectNote = "Партия забракована: превышен допустимый уровень брака"
	case defectRate > 3:
		result.Status = "rework"
		result.Defects = defects
		result.ReworkNote = "Партия требует доработки"
	default:
		result.Status = "passed"
	}

	return result
}

func generateSampleMeasurements(product string, count int) []Measurement {
	params := getProductParams(product)
	measurements := make([]Measurement, 0, count)
	for i := 0; i < count; i++ {
		for _, p := range params {
			m := Measurement{
				Param:    p.name,
				Nominal:  p.nominal,
				Value:    p.nominal + p.nominal*(rand.Float64()-0.5)*0.12,
				Unit:     p.unit,
				Critical: p.critical,
			}
			measurements = append(measurements, m)
		}
	}
	return measurements
}

type paramDef struct {
	name     string
	nominal  float64
	unit     string
	critical bool
}

func getProductParams(product string) []paramDef {
	switch strings.ToLower(product) {
	case "gear", "шестерня":
		return []paramDef{
			{name: "outer_diameter", nominal: 100.0, unit: "mm", critical: true},
			{name: "inner_diameter", nominal: 25.0, unit: "mm", critical: true},
			{name: "tooth_thickness", nominal: 5.0, unit: "mm", critical: false},
			{name: "hardness", nominal: 58.0, unit: "HRC", critical: true},
		}
	case "shaft", "вал":
		return []paramDef{
			{name: "length", nominal: 500.0, unit: "mm", critical: false},
			{name: "diameter", nominal: 40.0, unit: "mm", critical: true},
			{name: "roughness", nominal: 1.6, unit: "Ra", critical: false},
		}
	default:
		return []paramDef{
			{name: "length", nominal: 200.0, unit: "mm", critical: false},
			{name: "width", nominal: 100.0, unit: "mm", critical: false},
			{name: "weight", nominal: 1.5, unit: "kg", critical: false},
		}
	}
}
