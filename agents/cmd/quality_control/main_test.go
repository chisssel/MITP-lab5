package main

import (
	"math"
	"testing"
)

func TestInspectQuality_Passed(t *testing.T) {
	req := QCRequest{
		BatchID:  "BATCH-001",
		Product:  "Шестерня",
		Quantity: 100,
		Measurements: []Measurement{
			{Param: "outer_diameter", Value: 100.0, Nominal: 100.0, Unit: "mm", Critical: true},
			{Param: "inner_diameter", Value: 25.0, Nominal: 25.0, Unit: "mm", Critical: true},
			{Param: "tooth_thickness", Value: 5.0, Nominal: 5.0, Unit: "mm", Critical: false},
			{Param: "hardness", Value: 58.0, Nominal: 58.0, Unit: "HRC", Critical: true},
		},
		OrderID: "ORD-001",
	}

	result := inspectQuality(req)

	if result.Status != "passed" {
		t.Errorf("expected status=passed, got %s", result.Status)
	}
	if result.DefectRate != 0 {
		t.Errorf("expected 0 defects, got defect_rate=%f", result.DefectRate)
	}
}

func TestInspectQuality_AllWithinTolerance(t *testing.T) {
	req := QCRequest{
		BatchID:  "BATCH-002",
		Product:  "Вал",
		Quantity: 50,
		Measurements: []Measurement{
			{Param: "length", Value: 505.0, Nominal: 500.0, Unit: "mm", Critical: false},
			{Param: "diameter", Value: 40.3, Nominal: 40.0, Unit: "mm", Critical: true},
			{Param: "roughness", Value: 1.65, Nominal: 1.6, Unit: "Ra", Critical: false},
		},
	}

	result := inspectQuality(req)

	if result.Status != "passed" {
		t.Errorf("expected status=passed for within-tolerance, got %s", result.Status)
	}
}

func TestInspectQuality_Rework(t *testing.T) {
	req := QCRequest{
		BatchID:  "BATCH-003",
		Product:  "Шестерня",
		Quantity: 20,
		Measurements: []Measurement{
			{Param: "outer_diameter", Value: 108.0, Nominal: 100.0, Unit: "mm", Critical: true},
			{Param: "inner_diameter", Value: 25.0, Nominal: 25.0, Unit: "mm", Critical: true},
			{Param: "tooth_thickness", Value: 5.5, Nominal: 5.0, Unit: "mm", Critical: false},
			{Param: "hardness", Value: 58.0, Nominal: 58.0, Unit: "HRC", Critical: true},
		},
	}

	result := inspectQuality(req)

	if result.Status != "rework" && result.Status != "rejected" {
		t.Errorf("expected rework or rejected for out-of-tolerance, got %s", result.Status)
	}
}

func TestInspectQuality_CriticalDefectRejects(t *testing.T) {
	req := QCRequest{
		BatchID:  "BATCH-004",
		Product:  "Шестерня",
		Quantity: 10,
		Measurements: []Measurement{
			{Param: "outer_diameter", Value: 150.0, Nominal: 100.0, Unit: "mm", Critical: true},
			{Param: "hardness", Value: 30.0, Nominal: 58.0, Unit: "HRC", Critical: true},
		},
	}

	result := inspectQuality(req)

	if result.Status != "rejected" {
		t.Errorf("expected status=rejected for critical defects, got %s", result.Status)
	}
	if result.RejectNote == "" {
		t.Error("expected reject_note for rejected batch")
	}
}

func TestInspectQuality_NoMeasurementsGeneratesSample(t *testing.T) {
	req := QCRequest{
		BatchID:  "BATCH-005",
		Product:  "Шестерня",
		Quantity: 30,
	}

	result := inspectQuality(req)

	if len(result.Defects) == 0 && result.DefectRate > 0 {
		t.Log("sample measurements may have random defects — not an error")
	}
	if result.Status != "passed" && result.Status != "rework" && result.Status != "rejected" {
		t.Errorf("unexpected status: %s", result.Status)
	}
}

func TestInspectQuality_DeviationComputation(t *testing.T) {
	req := QCRequest{
		BatchID:  "BATCH-DEV",
		Product:  "Generic",
		Quantity: 10,
		Measurements: []Measurement{
			{Param: "length", Value: 220.0, Nominal: 200.0, Unit: "mm", Critical: false},
		},
	}

	result := inspectQuality(req)

	if len(result.Defects) > 0 {
		d := result.Defects[0]
		expectedDev := math.Round(math.Abs(220.0-200.0)/200.0*10000) / 100
		if d.Deviation != expectedDev {
			t.Errorf("expected deviation=%.2f, got %.2f", expectedDev, d.Deviation)
		}
	}
}

func TestGetProductParams(t *testing.T) {
	tests := []struct {
		product string
		want    int
	}{
		{"gear", 4},
		{"шестерня", 4},
		{"shaft", 3},
		{"вал", 3},
		{"unknown", 3},
		{"", 3},
	}

	for _, tt := range tests {
		params := getProductParams(tt.product)
		if len(params) != tt.want {
			t.Errorf("getProductParams(%q) returned %d params, want %d", tt.product, len(params), tt.want)
		}
	}
}

func TestDefectRateBoundaries(t *testing.T) {
	makeMeas := func(nominal, value float64, critical bool) Measurement {
		return Measurement{Param: "p", Value: value, Nominal: nominal, Unit: "mm", Critical: critical}
	}

	t.Run("0% defect rate", func(t *testing.T) {
		req := QCRequest{
			BatchID: "B-0", Product: "generic", Quantity: 10,
			Measurements: []Measurement{
				makeMeas(100, 100, false),
				makeMeas(100, 100, false),
				makeMeas(100, 100, false),
			},
		}
		r := inspectQuality(req)
		if r.DefectRate != 0 {
			t.Errorf("expected 0%% defect rate, got %f%%", r.DefectRate)
		}
	})

	t.Run("100% defect rate", func(t *testing.T) {
		req := QCRequest{
			BatchID: "B-100", Product: "generic", Quantity: 10,
			Measurements: []Measurement{
				makeMeas(100, 200, false),
				makeMeas(100, 200, false),
			},
		}
		r := inspectQuality(req)
		if r.DefectRate != 100 {
			t.Errorf("expected 100%% defect rate, got %f%%", r.DefectRate)
		}
	})
}
