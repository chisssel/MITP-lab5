import asyncio
import json
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from orchestrator import ProductionOrchestrator, setup_logging


@pytest.mark.asyncio
async def test_init_state():
    o = ProductionOrchestrator()
    assert o.nc is None
    assert o.results == {}
    assert o.subscription is None


@pytest.mark.asyncio
async def test_demo_mode():
    import io
    stream = io.StringIO()
    logger = setup_logging(stream=stream)
    o = ProductionOrchestrator(logger=logger)
    await o.demo_mode()

    output = stream.getvalue()
    assert "ДЕМО-РЕЖИМ" in output
    assert "Планирование загрузки" in output
    assert "Проверка запасов" in output
    assert "Диспетчеризация" in output
    assert "Контроль качества" in output


@pytest.mark.asyncio
async def test_on_result_sets_future():
    o = ProductionOrchestrator()
    future = asyncio.Future()
    o.results["test-task"] = future

    msg = MagicMock()
    payload = json.dumps({"task_id": "test-task", "success": True,
                          "output": "{}", "agent": "load_planner"})
    msg.data.decode.return_value = payload

    await o._on_result(msg)

    assert future.done()
    result = future.result()
    assert result["task_id"] == "test-task"
    assert result["success"] is True
    assert result["agent"] == "load_planner"


@pytest.mark.asyncio
async def test_on_result_unknown_task():
    o = ProductionOrchestrator()
    msg = MagicMock()
    payload = json.dumps({"task_id": "nobody-waits", "success": True,
                          "output": "{}", "agent": "ghost"})
    msg.data.decode.return_value = payload

    await o._on_result(msg)
    assert len(o.results) == 0


@pytest.mark.asyncio
async def test_send_task_timeout():
    o = ProductionOrchestrator()

    # Mock NATS client
    o.nc = AsyncMock()
    o.nc.publish = AsyncMock()
    o.subscription = MagicMock()

    o.nc.subscribe = AsyncMock()

    result = await o.send_task("production.test", "test", {"key": "value"}, timeout=0.1)

    assert result["success"] is False
    assert result["agent"] == "timeout"
    assert result["task_id"] != ""

    # Verify publish was called
    o.nc.publish.assert_called_once()
    call_args = o.nc.publish.call_args[0]
    assert call_args[0] == "production.test"


@pytest.mark.asyncio
async def test_send_task_success():
    o = ProductionOrchestrator()
    o.nc = AsyncMock()
    o.nc.publish = AsyncMock()
    o.subscription = MagicMock()

    async def delayed_set_result():
        await asyncio.sleep(0.05)
        task_id = list(o.results.keys())[0]
        o.results[task_id].set_result(
            {"task_id": task_id, "success": True, "output": '{"ok":true}', "agent": "test"}
        )

    async def run():
        result_future = asyncio.create_task(
            o.send_task("production.test", "test", {"key": "value"}, timeout=5)
        )
        await asyncio.create_task(delayed_set_result())
        return await result_future

    result = await run()

    assert result["success"] is True
    assert result["agent"] == "test"


@pytest.mark.asyncio
async def test_plan_production_format():
    """Verify the orchestrator sends planning requests with correct fields."""
    o = ProductionOrchestrator()
    o.nc = AsyncMock()
    o.nc.publish = AsyncMock()
    o.subscription = MagicMock()

    async def respond():
        await asyncio.sleep(0.05)
        task_id = list(o.results.keys())[0]
        o.results[task_id].set_result(
            {"task_id": task_id, "success": True,
             "output": json.dumps({"order_id": "ORD-T", "feasible": True,
                                   "schedule": [], "total_time_mins": 60,
                                   "utilization_pct": 50.0}),
             "agent": "load_planner"}
        )

    async def run():
        task = asyncio.create_task(
            o.plan_production({
                "order_id": "ORD-T", "product": "Test",
                "quantity": 100, "deadline": "12:00 01.01.2027",
                "priority": "high"
            })
        )
        await asyncio.create_task(respond())
        return await task

    result = await run()
    assert result["success"] is True
    output = json.loads(result["output"])
    assert output["feasible"] is True
    assert output["order_id"] == "ORD-T"


@pytest.mark.asyncio
async def test_quality_control_format():
    o = ProductionOrchestrator()
    o.nc = AsyncMock()
    o.nc.publish = AsyncMock()
    o.subscription = MagicMock()

    async def respond():
        await asyncio.sleep(0.05)
        task_id = list(o.results.keys())[0]
        o.results[task_id].set_result(
            {"task_id": task_id, "success": True,
             "output": json.dumps({"batch_id": "B-T", "status": "passed",
                                   "defect_rate": 0, "passed_pct": 100}),
             "agent": "quality_control"}
        )

    async def run():
        task = asyncio.create_task(
            o.quality_control("B-T", "Test", 100)
        )
        await asyncio.create_task(respond())
        return await task

    result = await run()
    assert result["success"] is True


@pytest.mark.asyncio
async def test_inventory_check_format():
    o = ProductionOrchestrator()
    o.nc = AsyncMock()
    o.nc.publish = AsyncMock()
    o.subscription = MagicMock()

    async def respond():
        await asyncio.sleep(0.05)
        task_id = list(o.results.keys())[0]
        o.results[task_id].set_result(
            {"task_id": task_id, "success": True,
             "output": json.dumps({"status": "ok", "available_qty": 5000}),
             "agent": "inventory"}
        )

    async def run():
        task = asyncio.create_task(
            o.check_inventory("steel", 100)
        )
        await asyncio.create_task(respond())
        return await task

    result = await run()
    assert result["success"] is True


@pytest.mark.asyncio
async def test_dispatch_format():
    o = ProductionOrchestrator()
    o.nc = AsyncMock()
    o.nc.publish = AsyncMock()
    o.subscription = MagicMock()

    async def respond():
        await asyncio.sleep(0.05)
        task_id = list(o.results.keys())[0]
        o.results[task_id].set_result(
            {"task_id": task_id, "success": True,
             "output": json.dumps({"dispatch_id": "DSP-1", "overall_status": "in_progress",
                                   "lines": [{"line": "A", "task": "Test", "status": "in_progress"}]}),
             "agent": "dispatcher"}
        )

    async def run():
        task = asyncio.create_task(
            o.dispatch_production("ORD-T", [{"machine": "A", "task": "Test", "duration_mins": 30}], "high")
        )
        await asyncio.create_task(respond())
        return await task

    result = await run()
    assert result["success"] is True
    output = json.loads(result["output"])
    assert output["dispatch_id"] == "DSP-1"
    assert output["lines"][0]["line"] == "A"


@pytest.mark.asyncio
async def test_run_production_cycle_partial_failure():
    """Cycle continues even if some steps fail."""
    o = ProductionOrchestrator()
    o.nc = AsyncMock()
    o.nc.publish = AsyncMock()
    o.subscription = MagicMock()

    # Only respond to the first task, let others timeout
    response_count = 0

    async def delayed_respond_one():
        nonlocal response_count
        await asyncio.sleep(0.1)
        if o.results:
            task_id = list(o.results.keys())[0]
            o.results[task_id].set_result(
                {"task_id": task_id, "success": True,
                 "output": json.dumps({"order_id": "ORD-F", "feasible": True,
                                       "schedule": [], "total_time_mins": 10,
                                       "utilization_pct": 10}),
                 "agent": "load_planner"}
            )
            response_count += 1

    async def run():
        task = asyncio.create_task(
            o.run_production_cycle({
                "order_id": "ORD-F", "product": "Test",
                "quantity": 10, "deadline": "12:00 01.01.2027",
                "priority": "normal"
            })
        )
        await asyncio.create_task(delayed_respond_one())
        result = await task
        return result

    result = await run()
    assert "planning" in result
    assert "inventory_check" in result
    assert response_count >= 1


@pytest.mark.asyncio
async def test_connect_with_invalid_nats(capsys):
    o = ProductionOrchestrator()
    import nats

    with patch("nats.connect", AsyncMock(side_effect=ConnectionError("NATS down"))):
        with pytest.raises(ConnectionError):
            await o.connect()


@pytest.mark.asyncio
async def test_concurrent_tasks():
    """Multiple tasks can be tracked simultaneously."""
    o = ProductionOrchestrator()
    o.nc = AsyncMock()
    o.nc.publish = AsyncMock()
    o.subscription = MagicMock()

    fut1 = asyncio.Future()
    fut2 = asyncio.Future()
    o.results["task-1"] = fut1
    o.results["task-2"] = fut2

    assert len(o.results) == 2

    msg1 = MagicMock()
    msg1.data.decode.return_value = json.dumps(
        {"task_id": "task-1", "success": True, "output": "{}", "agent": "a1"})
    msg2 = MagicMock()
    msg2.data.decode.return_value = json.dumps(
        {"task_id": "task-2", "success": True, "output": "{}", "agent": "a2"})

    await o._on_result(msg1)
    await o._on_result(msg2)

    assert fut1.done()
    assert fut2.done()
    assert len(o.results) == 0
