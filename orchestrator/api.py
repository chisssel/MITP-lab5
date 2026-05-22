import asyncio
import json
import logging
from datetime import datetime, timedelta
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from orchestrator import ProductionOrchestrator, setup_logging, Metrics


orchestrator: ProductionOrchestrator | None = None
background_loop: asyncio.AbstractEventLoop | None = None


class OrderRequest(BaseModel):
    order_id: str
    product: str
    quantity: int
    deadline: str | None = None
    priority: str = "high"


class MaterialRequest(BaseModel):
    material: str
    required_qty: int


class DispatchRequest(BaseModel):
    order_id: str
    schedule: list = []
    priority: str = "high"


class QCRequest(BaseModel):
    batch_id: str
    product: str
    quantity: int


@asynccontextmanager
async def lifespan(app: FastAPI):
    global orchestrator, background_loop
    logger = setup_logging()
    orchestrator = ProductionOrchestrator(logger=logger)
    background_loop = asyncio.get_event_loop()
    try:
        await orchestrator.connect()
    except Exception:
        logger.warning("NATS недоступен, используются заглушки")
        orchestrator.nc = None
    yield
    if orchestrator:
        await orchestrator.disconnect()


app = FastAPI(
    title="Production MAS API",
    description="REST API для мультиагентной системы управления производством",
    version="1.0.0",
    lifespan=lifespan,
)


@app.get("/")
async def root():
    return {
        "service": "Production MAS",
        "endpoints": {
            "POST /production/cycle": "Полный производственный цикл",
            "POST /production/planning": "Планирование загрузки",
            "POST /production/inventory": "Проверка запасов",
            "POST /production/dispatch": "Диспетчеризация",
            "POST /production/quality": "Контроль качества",
            "GET  /metrics": "Метрики оркестратора",
            "GET  /health": "Проверка здоровья",
        },
    }


@app.post("/production/cycle")
async def run_cycle(order: OrderRequest):
    if not orchestrator:
        raise HTTPException(503, "Оркестратор не инициализирован")

    deadline = order.deadline or (
        datetime.now() + timedelta(hours=8)
    ).strftime("%H:%M %d.%m.%Y")

    payload = {
        "order_id": order.order_id,
        "product": order.product,
        "quantity": order.quantity,
        "deadline": deadline,
        "priority": order.priority,
    }

    if orchestrator.nc:
        result = await orchestrator.run_production_cycle(payload)
    else:
        await orchestrator.demo_mode()
        result = {"status": "demo"}

    return {
        "order": payload,
        "metrics": orchestrator.metrics.summary(),
        "results": {
            k: {
                "success": v.get("success"),
                "output": json.loads(v.get("output", "{}"))
                if v.get("output") and v.get("success")
                else v.get("output"),
            }
            for k, v in result.items()
        } if isinstance(result, dict) else result,
    }


@app.post("/production/planning")
async def api_planning(order: OrderRequest):
    if not orchestrator or not orchestrator.nc:
        raise HTTPException(503, "NATS недоступен")
    deadline = order.deadline or (
        datetime.now() + timedelta(hours=8)
    ).strftime("%H:%M %d.%m.%Y")
    result = await orchestrator.plan_production({
        "order_id": order.order_id, "product": order.product,
        "quantity": order.quantity, "deadline": deadline,
        "priority": order.priority,
    })
    return {
        "success": result.get("success"),
        "result": json.loads(result.get("output", "{}"))
        if result.get("success") else result,
    }


@app.post("/production/inventory")
async def api_inventory(req: MaterialRequest):
    if not orchestrator or not orchestrator.nc:
        raise HTTPException(503, "NATS недоступен")
    result = await orchestrator.check_inventory(req.material, req.required_qty)
    return {
        "success": result.get("success"),
        "result": json.loads(result.get("output", "{}"))
        if result.get("success") else result,
    }


@app.post("/production/dispatch")
async def api_dispatch(req: DispatchRequest):
    if not orchestrator or not orchestrator.nc:
        raise HTTPException(503, "NATS недоступен")
    result = await orchestrator.dispatch_production(
        req.order_id, req.schedule, req.priority
    )
    return {
        "success": result.get("success"),
        "result": json.loads(result.get("output", "{}"))
        if result.get("success") else result,
    }


@app.post("/production/quality")
async def api_quality(req: QCRequest):
    if not orchestrator or not orchestrator.nc:
        raise HTTPException(503, "NATS недоступен")
    result = await orchestrator.quality_control(
        req.batch_id, req.product, req.quantity
    )
    return {
        "success": result.get("success"),
        "result": json.loads(result.get("output", "{}"))
        if result.get("success") else result,
    }


@app.get("/metrics")
async def get_metrics():
    return orchestrator.metrics.summary() if orchestrator else Metrics().summary()


@app.get("/health")
async def health():
    return {
        "status": "ok" if orchestrator and orchestrator.nc else "degraded",
        "nats": orchestrator.nc is not None if orchestrator else False,
    }
