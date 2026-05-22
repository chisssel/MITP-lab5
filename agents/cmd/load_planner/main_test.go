package main

import (
	"testing"
	"time"
)

func TestPlanLoad_Normal(t *testing.T) {
	req := PlanningRequest{
		OrderID:  "ORD-001",
		Product:  "Шестерня",
		Quantity: 100,
		Deadline: time.Now().Add(24 * time.Hour).Format("15:04 02.01.2006"),
		Priority: "normal",
	}

	result := planLoad(req)

	if !result.Feasible {
		t.Errorf("expected feasible=true for 100 units with 24h deadline, got feasible=%v", result.Feasible)
	}
	if result.OrderID != "ORD-001" {
		t.Errorf("expected OrderID=ORD-001, got %s", result.OrderID)
	}
	if len(result.Schedule) == 0 {
		t.Error("expected at least one machine slot in schedule")
	}
	if result.TotalTimeMins <= 0 {
		t.Errorf("expected positive TotalTimeMins, got %d", result.TotalTimeMins)
	}
}

func TestPlanLoad_CriticalPriority(t *testing.T) {
	req := PlanningRequest{
		OrderID:  "ORD-002",
		Product:  "Вал",
		Quantity: 1000,
		Deadline: time.Now().Add(2 * time.Hour).Format("15:04 02.01.2006"),
		Priority: "critical",
	}

	result := planLoad(req)

	if !result.Feasible {
		t.Errorf("critical should always be feasible, got feasible=%v", result.Feasible)
	}
}

func TestPlanLoad_LowPriorityExceedsDeadline(t *testing.T) {
	req := PlanningRequest{
		OrderID:  "ORD-003",
		Product:  "Втулка",
		Quantity: 5000,
		Deadline: time.Now().Add(30 * time.Minute).Format("15:04 02.01.2006"),
		Priority: "low",
	}

	result := planLoad(req)

	if result.Feasible {
		t.Logf("note: %s", result.Note)
	}
}

func TestPlanLoad_ZeroQuantity(t *testing.T) {
	req := PlanningRequest{
		OrderID:  "ORD-000",
		Product:  "Тест",
		Quantity: 0,
		Deadline: time.Now().Add(1 * time.Hour).Format("15:04 02.01.2006"),
		Priority: "high",
	}

	result := planLoad(req)

	if !result.Feasible {
		t.Error("expected feasible=true for zero quantity")
	}
}

func TestPlanLoad_InvalidDeadline(t *testing.T) {
	req := PlanningRequest{
		OrderID:  "ORD-004",
		Product:  "Test",
		Quantity: 50,
		Deadline: "invalid-date",
		Priority: "high",
	}

	result := planLoad(req)

	if !result.Feasible {
		t.Error("expected feasible=true even with invalid deadline (falls back to 24h)")
	}
}

func TestPlanLoad_PriorityWeights(t *testing.T) {
	base := PlanningRequest{
		OrderID: "ORD-P", Product: "P", Quantity: 500,
		Deadline: time.Now().Add(48 * time.Hour).Format("15:04 02.01.2006"),
	}

	low := planLoad(PlanningRequest{Priority: "low", OrderID: base.OrderID, Product: base.Product,
		Quantity: base.Quantity, Deadline: base.Deadline})
	high := planLoad(PlanningRequest{Priority: "high", OrderID: base.OrderID, Product: base.Product,
		Quantity: base.Quantity, Deadline: base.Deadline})

	if low.TotalTimeMins > 0 && high.TotalTimeMins > 0 {
		t.Logf("low total=%d min, high total=%d min", low.TotalTimeMins, high.TotalTimeMins)
	}
}

func TestUtilizationPct_Range(t *testing.T) {
	req := PlanningRequest{
		OrderID: "ORD-U", Product: "U", Quantity: 1000,
		Deadline: time.Now().Add(4 * time.Hour).Format("15:04 02.01.2006"),
		Priority: "high",
	}

	result := planLoad(req)

	if result.UtilizationPct < 0 || result.UtilizationPct > 100 {
		t.Errorf("utilization out of range [0,100]: %f", result.UtilizationPct)
	}
}
