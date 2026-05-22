package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math"
	"math/rand"
	"time"

	"lab5/agents/shared"

	"github.com/nats-io/nats.go"
)

type PlanningRequest struct {
	OrderID  string `json:"order_id"`
	Product  string `json:"product"`
	Quantity int    `json:"quantity"`
	Deadline string `json:"deadline"`
	Priority string `json:"priority"`
}

type MachineSlot struct {
	Machine    string `json:"machine"`
	StartTime  string `json:"start_time"`
	EndTime    string `json:"end_time"`
	Task       string `json:"task"`
	QtyPerHour int    `json:"qty_per_hour"`
}

type PlanningResult struct {
	OrderID       string        `json:"order_id"`
	Feasible      bool          `json:"feasible"`
	Schedule      []MachineSlot `json:"schedule"`
	TotalTimeMins int           `json:"total_time_mins"`
	UtilizationPct float64      `json:"utilization_pct"`
	Note          string        `json:"note,omitempty"`
}

type Machine struct {
	Name      string
	QtyPerHour int
	Available bool
}

var machines = []Machine{
	{Name: "CNC-1", QtyPerHour: 50, Available: true},
	{Name: "CNC-2", QtyPerHour: 40, Available: true},
	{Name: "Assembly-1", QtyPerHour: 80, Available: false},
	{Name: "Assembly-2", QtyPerHour: 70, Available: true},
	{Name: "Packing-1", QtyPerHour: 120, Available: true},
}

func main() {
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		log.Fatalf("Ошибка подключения к NATS: %v", err)
	}
	defer nc.Close()

	log.Println("[LoadPlanner] Агент запущен, ожидание задач...")

	nc.Subscribe("production.planning", func(m *nats.Msg) {
		var task shared.Task
		if err := json.Unmarshal(m.Data, &task); err != nil {
			log.Printf("Ошибка разбора задачи: %v", err)
			return
		}

		log.Printf("Получена задача %s типа %s", task.ID, task.Type)

		var req PlanningRequest
		if err := json.Unmarshal([]byte(task.Payload), &req); err != nil {
			log.Printf("Ошибка разбора payload: %v", err)
			return
		}

		result := planLoad(req)
		output, _ := json.Marshal(result)
		log.Printf("Задача %s выполнена: feasible=%v", task.ID, result.Feasible)

		res := shared.Result{
			TaskID:  task.ID,
			Success: true,
			Output:  string(output),
			Agent:   "load_planner",
		}
		response, _ := json.Marshal(res)
		nc.Publish("production.completed", response)
	})

	select {}
}

func planLoad(req PlanningRequest) PlanningResult {
	deadline, err := time.Parse("15:04 02.01.2006", req.Deadline)
	now := time.Now()
	if err != nil {
		deadline = now.Add(24 * time.Hour)
	}

	availableMins := deadline.Sub(now).Minutes()
	if availableMins < 0 {
		availableMins = 60
	}

	priorityWeight := 1.0
	switch req.Priority {
	case "critical":
		priorityWeight = 2.0
	case "high":
		priorityWeight = 1.5
	case "normal":
		priorityWeight = 1.0
	case "low":
		priorityWeight = 0.5
	}

	var schedule []MachineSlot
	totalMins := 0.0
	remaining := req.Quantity

	rand.Shuffle(len(machines), func(i, j int) {
		machines[i], machines[j] = machines[j], machines[i]
	})

	for _, m := range machines {
		if remaining <= 0 {
			break
		}

		if !m.Available {
			continue
		}

		effectiveRate := float64(m.QtyPerHour) * priorityWeight
		qtyThis := int(math.Min(float64(remaining), float64(m.QtyPerHour)*8))
		timeNeeded := float64(qtyThis) / effectiveRate * 60

		const breakMin = 15.0
		totalNeeded := timeNeeded + breakMin

		endTime := now.Add(time.Duration(totalMins+totalNeeded) * time.Minute)
		if endTime.After(deadline) && req.Priority != "critical" {
			maxTime := availableMins - totalMins - breakMin
			if maxTime <= 0 {
				continue
			}
			qtyThis = int(effectiveRate * maxTime / 60)
			timeNeeded = maxTime
			totalNeeded = timeNeeded + breakMin
		}

		if qtyThis <= 0 {
			continue
		}

		slot := MachineSlot{
			Machine:    m.Name,
			StartTime:  now.Add(time.Duration(totalMins) * time.Minute).Format("15:04 02.01.2006"),
			EndTime:    now.Add(time.Duration(totalMins+timeNeeded) * time.Minute).Format("15:04 02.01.2006"),
			Task:       fmt.Sprintf("Производство %s: %d ед.", req.Product, qtyThis),
			QtyPerHour: m.QtyPerHour,
		}
		schedule = append(schedule, slot)
		totalMins += totalNeeded
		remaining -= qtyThis
	}

	result := PlanningResult{
		OrderID:   req.OrderID,
		Schedule:  schedule,
		Feasible:  remaining <= 0,
		TotalTimeMins: int(math.Ceil(totalMins)),
	}

	if len(machines) > 0 {
		utilization := (totalMins / float64(len(machines))) / availableMins * 100
		if utilization > 100 {
			utilization = 100
		}
		result.UtilizationPct = math.Round(utilization*100) / 100
	}

	if !result.Feasible {
		neededMore := remaining
		result.Note = fmt.Sprintf(
			"Невозможно выполнить заказ в срок. Не хватает мощностей на %d ед. "+
				"Рекомендуется увеличить срок на %.0f ч или снизить объём.",
			neededMore, float64(neededMore)/80,
		)
	}

	return result
}
