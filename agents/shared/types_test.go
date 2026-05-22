package shared

import (
	"encoding/json"
	"testing"
)

func TestTaskJSONRoundtrip(t *testing.T) {
	task := Task{
		ID:      "test-1",
		Type:    "planning",
		Payload: `{"order_id":"ORD-001","product":"gear","quantity":100}`,
	}

	data, err := json.Marshal(task)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}

	var decoded Task
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if decoded.ID != task.ID {
		t.Errorf("ID: got %q, want %q", decoded.ID, task.ID)
	}
	if decoded.Type != task.Type {
		t.Errorf("Type: got %q, want %q", decoded.Type, task.Type)
	}
	if decoded.Payload != task.Payload {
		t.Errorf("Payload: got %q, want %q", decoded.Payload, task.Payload)
	}
}

func TestResultJSONRoundtrip(t *testing.T) {
	result := Result{
		TaskID:  "test-1",
		Success: true,
		Output:  `{"feasible":true,"total_time_mins":120}`,
		Agent:   "load_planner",
	}

	data, err := json.Marshal(result)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}

	var decoded Result
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if decoded.TaskID != result.TaskID {
		t.Errorf("TaskID: got %q, want %q", decoded.TaskID, result.TaskID)
	}
	if decoded.Success != result.Success {
		t.Errorf("Success: got %v, want %v", decoded.Success, result.Success)
	}
	if decoded.Agent != result.Agent {
		t.Errorf("Agent: got %q, want %q", decoded.Agent, result.Agent)
	}
}

func TestTaskWithEmptyPayload(t *testing.T) {
	task := Task{
		ID:      "empty",
		Type:    "ping",
		Payload: "{}",
	}

	data, _ := json.Marshal(task)
	var decoded Task
	json.Unmarshal(data, &decoded)

	if decoded.Payload != "{}" {
		t.Errorf("expected empty object, got %q", decoded.Payload)
	}
}

func TestResultFailure(t *testing.T) {
	result := Result{
		TaskID:  "failed-task",
		Success: false,
		Output:  `{"error":"timeout"}`,
		Agent:   "timeout",
	}

	data, _ := json.Marshal(result)
	var decoded Result
	json.Unmarshal(data, &decoded)

	if decoded.Success != false {
		t.Error("expected Success=false")
	}
	if decoded.Agent != "timeout" {
		t.Errorf("expected agent=timeout, got %q", decoded.Agent)
	}
}
