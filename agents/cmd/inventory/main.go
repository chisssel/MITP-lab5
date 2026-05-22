package main

import (
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"time"

	"lab5/agents/shared"

	"github.com/nats-io/nats.go"
)

type InventoryRequest struct {
	RequestType string `json:"request_type"`
	Material    string `json:"material"`
	RequiredQty int    `json:"required_qty"`
	WarehouseID string `json:"warehouse_id"`
}

type InventoryResult struct {
	RequestID        string `json:"request_id"`
	Status           string `json:"status"`
	Material         string `json:"material"`
	AvailableQty     int    `json:"available_qty"`
	ReservedQty      int    `json:"reserved_qty"`
	SafetyStock      int    `json:"safety_stock"`
	ReorderUntil     string `json:"reorder_until,omitempty"`
	EstimatedArrival string `json:"estimated_arrival,omitempty"`
	Note             string `json:"note,omitempty"`
}

type StockItem struct {
	Material     string
	Available    int
	Reserved     int
	AvgMonthly   int
	LeadTimeDays int
}

var (
	warehouse     []StockItem
	warehouseLock sync.Mutex
	initOnce      sync.Once
)

func initWarehouse() {
	warehouse = []StockItem{
		{Material: "steel_1018", Available: 5000, Reserved: 200, AvgMonthly: 3000, LeadTimeDays: 7},
		{Material: "steel_1045", Available: 3000, Reserved: 100, AvgMonthly: 2000, LeadTimeDays: 10},
		{Material: "aluminum_6061", Available: 2000, Reserved: 150, AvgMonthly: 1500, LeadTimeDays: 5},
		{Material: "copper_wire", Available: 800, Reserved: 50, AvgMonthly: 400, LeadTimeDays: 14},
		{Material: "plastic_pellets", Available: 4000, Reserved: 300, AvgMonthly: 2500, LeadTimeDays: 3},
		{Material: "bolts_m8", Available: 10000, Reserved: 500, AvgMonthly: 5000, LeadTimeDays: 2},
		{Material: "nuts_m8", Available: 10000, Reserved: 400, AvgMonthly: 5000, LeadTimeDays: 2},
		{Material: "paint_black", Available: 200, Reserved: 20, AvgMonthly: 150, LeadTimeDays: 7},
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
	logger, err := shared.NewAgentLogger("inventory", logDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "logger init: %v\n", err)
		os.Exit(1)
	}
	defer logger.Close()

	metrics := shared.NewMetrics("inventory", logger)
	logger.Info("Агент запущен, ожидание задач...")

	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		logger.Error("Ошибка подключения к NATS: %v", err)
		os.Exit(1)
	}
	defer nc.Close()

	nc.Subscribe("production.inventory", func(m *nats.Msg) {
		start := time.Now()
		metrics.IncReceived()

		initOnce.Do(initWarehouse)

		var task shared.Task
		if err := json.Unmarshal(m.Data, &task); err != nil {
			logger.Error("Ошибка разбора задачи: %v", err)
			metrics.IncFailed()
			return
		}

		logger.Info("Получена задача %s типа %s", task.ID, task.Type)

		var req InventoryRequest
		if err := json.Unmarshal([]byte(task.Payload), &req); err != nil {
			logger.Error("Ошибка разбора payload задачи %s: %v", task.ID, err)
			metrics.IncFailed()
			return
		}

		logger.Info("Обработка: type=%s material=%s qty=%d",
			req.RequestType, req.Material, req.RequiredQty)

		result := manageInventory(req)
		output, _ := json.Marshal(result)
		logger.Info("Задача %s выполнена: status=%s material=%s",
			task.ID, result.Status, result.Material)

		res := shared.Result{
			TaskID:  task.ID,
			Success: true,
			Output:  string(output),
			Agent:   "inventory",
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

func manageInventory(req InventoryRequest) InventoryResult {
	warehouseLock.Lock()
	defer warehouseLock.Unlock()

	res := InventoryResult{
		RequestID: req.RequestType + "_" + req.Material,
		Material:  req.Material,
	}

	idx := -1
	for i, item := range warehouse {
		if item.Material == req.Material {
			idx = i
			break
		}
	}

	if idx == -1 {
		res.Status = "unknown"
		res.Note = "Материал не найден на складе"
		return res
	}

	item := &warehouse[idx]
	safetyStock := int(math.Ceil(float64(item.AvgMonthly) * 0.15))

	res.AvailableQty = item.Available
	res.ReservedQty = item.Reserved
	res.SafetyStock = safetyStock

	switch req.RequestType {
	case "check":
		effectiveAvailable := item.Available - item.Reserved - safetyStock
		if effectiveAvailable >= req.RequiredQty {
			res.Status = "ok"
			res.Note = "Достаточно запасов для выполнения заказа"
		} else {
			shortage := req.RequiredQty - effectiveAvailable
			if shortage < 0 {
				shortage = 0
			}
			res.Status = "shortage"
			reorderQty := shortage + safetyStock + req.RequiredQty
			arrival := time.Now().Add(time.Duration(item.LeadTimeDays) * 24 * time.Hour)
			res.ReorderUntil = arrival.Format("15:04 02.01.2006")
			res.EstimatedArrival = arrival.Format("15:04 02.01.2006")
			res.Note = "Дефицит. Оформлен заказ поставщику"

			item.Available += reorderQty
		}

	case "reserve":
		if item.Available-item.Reserved >= req.RequiredQty {
			item.Reserved += req.RequiredQty
			res.ReservedQty = item.Reserved
			res.Status = "ok"
			res.Note = "Материал зарезервирован под заказ"
		} else if item.Available >= req.RequiredQty {
			item.Reserved += req.RequiredQty
			res.ReservedQty = item.Reserved
			res.Status = "ok"
			res.Note = "Зарезервировано с использованием страхового запаса"
		} else {
			res.Status = "shortage"
			res.EstimatedArrival = time.Now().Add(
				time.Duration(item.LeadTimeDays) * 24 * time.Hour,
			).Format("15:04 02.01.2006")
			res.Note = fmt.Sprintf("Недостаточно для резерва. Дефицит: %d ед.", req.RequiredQty-item.Available)
		}

	case "restock":
		reorderQty := req.RequiredQty + safetyStock
		item.Available += reorderQty
		arrival := time.Now().Add(time.Duration(item.LeadTimeDays) * 24 * time.Hour).Add(
			time.Duration(rand.Intn(24)) * time.Hour)
		res.AvailableQty = item.Available
		res.Status = "restock_ordered"
		res.EstimatedArrival = arrival.Format("15:04 02.01.2006")
		res.Note = "Пополнение склада выполнено"

	default:
		res.Status = "error"
		res.Note = "Неизвестный тип запроса: " + req.RequestType
	}

	return res
}
