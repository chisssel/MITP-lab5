package main

import (
	"testing"
)

func resetWarehouse() {
	warehouse = []StockItem{
		{Material: "steel_1018", Available: 5000, Reserved: 200, AvgMonthly: 3000, LeadTimeDays: 7},
		{Material: "steel_1045", Available: 3000, Reserved: 100, AvgMonthly: 2000, LeadTimeDays: 10},
		{Material: "aluminum_6061", Available: 2000, Reserved: 150, AvgMonthly: 1500, LeadTimeDays: 5},
		{Material: "plastic_pellets", Available: 4000, Reserved: 300, AvgMonthly: 2500, LeadTimeDays: 3},
	}
}

func TestManageInventory_CheckOk(t *testing.T) {
	resetWarehouse()

	req := InventoryRequest{
		RequestType: "check",
		Material:    "steel_1018",
		RequiredQty: 500,
	}

	result := manageInventory(req)

	if result.Status != "ok" {
		t.Errorf("expected status=ok for sufficient stock, got %s", result.Status)
	}
	if result.Material != "steel_1018" {
		t.Errorf("expected material=steel_1018, got %s", result.Material)
	}
	if result.AvailableQty != 5000 {
		t.Errorf("expected available=5000, got %d", result.AvailableQty)
	}
	expectedSafety := int(3000 * 0.15) // ceil(450)
	if result.SafetyStock != expectedSafety {
		t.Errorf("expected safety_stock=%d, got %d", expectedSafety, result.SafetyStock)
	}
}

func TestManageInventory_CheckShortage(t *testing.T) {
	resetWarehouse()

	req := InventoryRequest{
		RequestType: "check",
		Material:    "steel_1018",
		RequiredQty: 50000,
	}

	result := manageInventory(req)

	if result.Status != "shortage" {
		t.Errorf("expected status=shortage, got %s", result.Status)
	}
	if result.Note == "" {
		t.Error("expected note for shortage")
	}
}

func TestManageInventory_ReserveOk(t *testing.T) {
	resetWarehouse()

	req := InventoryRequest{
		RequestType: "reserve",
		Material:    "aluminum_6061",
		RequiredQty: 500,
	}

	result := manageInventory(req)

	if result.Status != "ok" {
		t.Errorf("expected status=ok, got %s", result.Status)
	}
	if result.ReservedQty < 500+150 {
		t.Errorf("expected reserved >= 650 (150 initial + 500), got %d", result.ReservedQty)
	}
}

func TestManageInventory_ReserveShortage(t *testing.T) {
	resetWarehouse()

	req := InventoryRequest{
		RequestType: "reserve",
		Material:    "plastic_pellets",
		RequiredQty: 99999,
	}

	result := manageInventory(req)

	if result.Status != "shortage" {
		t.Errorf("expected status=shortage, got %s", result.Status)
	}
}

func TestManageInventory_Restock(t *testing.T) {
	resetWarehouse()
	before := warehouse[0].Available

	req := InventoryRequest{
		RequestType: "restock",
		Material:    "steel_1018",
		RequiredQty: 1000,
	}

	result := manageInventory(req)

	if result.Status != "restock_ordered" {
		t.Errorf("expected status=restock_ordered, got %s", result.Status)
	}
	expectedQty := before + 1000 + int(3000*0.15)
	if result.AvailableQty != expectedQty {
		t.Errorf("expected available=%d, got %d", expectedQty, result.AvailableQty)
	}
}

func TestManageInventory_UnknownMaterial(t *testing.T) {
	resetWarehouse()

	req := InventoryRequest{
		RequestType: "check",
		Material:    "unobtainium",
		RequiredQty: 10,
	}

	result := manageInventory(req)

	if result.Status != "unknown" {
		t.Errorf("expected status=unknown, got %s", result.Status)
	}
}

func TestManageInventory_UnknownRequestType(t *testing.T) {
	resetWarehouse()

	req := InventoryRequest{
		RequestType: "dance",
		Material:    "steel_1018",
		RequiredQty: 100,
	}

	result := manageInventory(req)

	if result.Status != "error" {
		t.Errorf("expected status=error, got %s", result.Status)
	}
}

func TestManageInventory_CheckZeroQuantity(t *testing.T) {
	resetWarehouse()

	req := InventoryRequest{
		RequestType: "check",
		Material:    "steel_1018",
		RequiredQty: 0,
	}

	result := manageInventory(req)

	if result.Status != "ok" {
		t.Errorf("expected status=ok for zero qty, got %s", result.Status)
	}
}

func TestManageInventory_ReserveUpdatesWarehouse(t *testing.T) {
	resetWarehouse()

	manageInventory(InventoryRequest{
		RequestType: "reserve", Material: "steel_1018", RequiredQty: 300,
	})

	check := manageInventory(InventoryRequest{
		RequestType: "check", Material: "steel_1018", RequiredQty: 500,
	})

	if check.Status != "ok" {
		t.Errorf("expected check ok after reserve: %s", check.Status)
	}
}
