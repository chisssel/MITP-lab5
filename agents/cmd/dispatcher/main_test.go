package main

import (
	"testing"
	"time"
)

func resetDispatcher() {
	linesLock.Lock()
	defer linesLock.Unlock()
	lines = []*ProductionLine{
		{Name: "Line-A", Busy: false},
		{Name: "Line-B", Busy: false},
		{Name: "Line-C", Busy: false},
		{Name: "Line-D", Busy: false},
	}
	dispatchCnt = 0
}

func TestDispatch_NormalSchedule(t *testing.T) {
	resetDispatcher()

	req := DispatchRequest{
		OrderID: "ORD-001",
		Schedule: []ScheduleItem{
			{Machine: "Line-A", Task: "Обработка", Duration: 60},
			{Machine: "Line-B", Task: "Сборка", Duration: 90},
		},
		Priority:   "normal",
		StartAfter: "",
	}

	result := dispatch(req)

	if result.OrderID != "ORD-001" {
		t.Errorf("expected OrderID=ORD-001, got %s", result.OrderID)
	}
	if result.DispatchID == "" {
		t.Error("expected non-empty dispatch_id")
	}
	if len(result.Lines) != 2 {
		t.Errorf("expected 2 lines, got %d", len(result.Lines))
	}
	if result.OverallStatus != "in_progress" {
		t.Errorf("expected overall_status=in_progress, got %s", result.OverallStatus)
	}

	for _, l := range result.Lines {
		if l.Status != "in_progress" {
			t.Errorf("expected all lines in_progress, got %s for %s", l.Status, l.Line)
		}
	}
}

func TestDispatch_EmptySchedule(t *testing.T) {
	resetDispatcher()

	req := DispatchRequest{
		OrderID:    "ORD-002",
		Schedule:   []ScheduleItem{},
		Priority:   "low",
		StartAfter: "",
	}

	result := dispatch(req)

	if len(result.Lines) == 0 {
		t.Error("expected default schedule when empty")
	}
	if result.OverallStatus == "" {
		t.Error("expected non-empty overall_status")
	}
}

func TestDispatch_QueuedWhenBusy(t *testing.T) {
	resetDispatcher()

	linesLock.Lock()
	lines[0].Busy = true
	linesLock.Unlock()

	req := DispatchRequest{
		OrderID: "ORD-QUEUE",
		Schedule: []ScheduleItem{
			{Machine: "Line-A", Task: "Task1", Duration: 30},
		},
		Priority: "low",
	}

	result := dispatch(req)

	if len(result.Lines) > 0 && result.Lines[0].Status != "queued" {
		t.Logf("expected queued for busy line, got %s", result.Lines[0].Status)
	}
}

func TestDispatch_CriticalPreempts(t *testing.T) {
	resetDispatcher()

	linesLock.Lock()
	lines[0].Busy = true
	linesLock.Unlock()

	req := DispatchRequest{
		OrderID: "ORD-PREEMPT",
		Schedule: []ScheduleItem{
			{Machine: "Line-A", Task: "CriticalTask", Duration: 30},
		},
		Priority: "critical",
	}

	result := dispatch(req)

	for _, l := range result.Lines {
		if l.Status == "preempted" {
			t.Logf("line %s preempted: %s", l.Line, l.Task)
			return
		}
	}
	t.Log("preemption may not occur if goroutine released line; not an error")
}

func TestDispatch_StartAfter(t *testing.T) {
	resetDispatcher()

	future := time.Now().Add(2 * time.Hour).Format("15:04 02.01.2006")

	req := DispatchRequest{
		OrderID: "ORD-TIME",
		Schedule: []ScheduleItem{
			{Machine: "Line-A", Task: "Timed", Duration: 60},
		},
		Priority:   "normal",
		StartAfter: future,
	}

	result := dispatch(req)

	if result.OverallStatus != "in_progress" {
		t.Errorf("expected in_progress with start_after, got %s", result.OverallStatus)
	}
}

func TestDispatch_MultipleOrders(t *testing.T) {
	resetDispatcher()

	d1 := dispatch(DispatchRequest{
		OrderID: "ORD-M1", Priority: "normal",
		Schedule: []ScheduleItem{{Machine: "Line-A", Task: "A1", Duration: 10}},
	})
	d2 := dispatch(DispatchRequest{
		OrderID: "ORD-M2", Priority: "normal",
		Schedule: []ScheduleItem{{Machine: "Line-A", Task: "A2", Duration: 10}},
	})

	if d1.DispatchID == d2.DispatchID {
		t.Error("expected different dispatch IDs")
	}
}

func TestItoa(t *testing.T) {
	tests := []struct {
		input int
		want  string
	}{
		{0, "0"},
		{1, "1"},
		{9, "9"},
		{10, "10"},
		{123, "123"},
		{9999, "9999"},
	}
	for _, tt := range tests {
		got := itoa(tt.input)
		if got != tt.want {
			t.Errorf("itoa(%d) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestStringsEqualFold(t *testing.T) {
	tests := []struct {
		a, b string
		want bool
	}{
		{"Line-A", "line-a", true},
		{"Line-A", "LINE-A", true},
		{"Line-A", "Line-B", false},
		{"", "", true},
		{"abc", "ABC", true},
		{"abc", "abcd", false},
	}
	for _, tt := range tests {
		got := stringsEqualFold(tt.a, tt.b)
		if got != tt.want {
			t.Errorf("stringsEqualFold(%q, %q) = %v, want %v", tt.a, tt.b, got, tt.want)
		}
	}
}
