import asyncio
import json
import logging
import os
import sys
import uuid
from datetime import datetime, timedelta
from typing import Dict, Optional

import nats


LOG_DIR = os.environ.get("ORCHESTRATOR_LOG_DIR", "logs")


def setup_logging(stream=None):
    os.makedirs(LOG_DIR, exist_ok=True)

    logger = logging.getLogger("orchestrator")
    logger.setLevel(logging.DEBUG)

    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)-5s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    console = logging.StreamHandler(stream or sys.stdout)
    console.setLevel(logging.INFO)
    console.setFormatter(formatter)

    fh = logging.FileHandler(
        os.path.join(LOG_DIR, "orchestrator.log"),
        encoding="utf-8",
    )
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)

    logger.handlers.clear()
    logger.addHandler(console)
    logger.addHandler(fh)

    return logger


class Metrics:
    def __init__(self):
        self.tasks_sent = 0
        self.tasks_completed = 0
        self.tasks_timeout = 0
        self.tasks_failed = 0
        self.start_time = datetime.now()

    def inc_sent(self):
        self.tasks_sent += 1

    def inc_completed(self):
        self.tasks_completed += 1

    def inc_timeout(self):
        self.tasks_timeout += 1

    def inc_failed(self):
        self.tasks_failed += 1

    def report(self) -> str:
        elapsed = datetime.now() - self.start_time
        return (
            f"=== МЕТРИКИ === отправлено={self.tasks_sent} "
            f"успешно={self.tasks_completed} таймаут={self.tasks_timeout} "
            f"ошибок={self.tasks_failed} uptime={elapsed}"
        )

    def summary(self) -> dict:
        return {
            "tasks_sent": self.tasks_sent,
            "tasks_completed": self.tasks_completed,
            "tasks_timeout": self.tasks_timeout,
            "tasks_failed": self.tasks_failed,
            "uptime_sec": (datetime.now() - self.start_time).total_seconds(),
        }


class ProductionOrchestrator:
    def __init__(self, logger=None):
        self.logger = logger or logging.getLogger("orchestrator")
        self.metrics = Metrics()
        self.nc: Optional[nats.NATS] = None
        self.results: Dict[str, asyncio.Future] = {}
        self.subscription = None

    async def connect(self):
        try:
            self.nc = await nats.connect("nats://localhost:4222")
            self.subscription = await self.nc.subscribe(
                "production.completed", cb=self._on_result
            )
            self.logger.info("Подключён к NATS, слушаю production.completed")
        except Exception as e:
            self.logger.error("Ошибка подключения к NATS: %s", e)
            raise

    async def disconnect(self):
        if self.subscription:
            await self.subscription.unsubscribe()
        if self.nc:
            await self.nc.drain()
            self.logger.info("Отключён от NATS")

    async def _on_result(self, msg):
        data = json.loads(msg.data.decode())
        task_id = data.get("task_id")
        agent = data.get("agent", "?")
        success = data.get("success")

        if task_id in self.results:
            future = self.results.pop(task_id)
            if not future.done():
                future.set_result(data)
            self.logger.info(
                "Получен результат от %s: task=%s success=%s",
                agent, task_id, success,
            )
            if success:
                self.metrics.inc_completed()
            else:
                self.metrics.inc_failed()
        else:
            self.logger.warning("Получен результат для неизвестной задачи %s", task_id)

    async def send_task(
        self, subject: str, task_type: str, payload: dict, timeout: int = 30
    ) -> dict:
        task_id = str(uuid.uuid4())
        task = {"id": task_id, "type": task_type, "payload": json.dumps(payload)}

        future = asyncio.Future()
        self.results[task_id] = future

        await self.nc.publish(subject, json.dumps(task).encode())
        self.metrics.inc_sent()
        self.logger.info(
            "Отправлена задача %s типа '%s' -> %s (таймаут=%dс)",
            task_id, task_type, subject, timeout,
        )

        try:
            result = await asyncio.wait_for(future, timeout)
            return result
        except asyncio.TimeoutError:
            self.results.pop(task_id, None)
            self.metrics.inc_timeout()
            self.logger.error(
                "ТАЙМАУТ: задача %s не выполнена за %d сек", task_id, timeout
            )
            return {
                "task_id": task_id,
                "success": False,
                "output": "{}",
                "agent": "timeout",
            }

    # ---- workflow steps ----

    async def plan_production(self, order: dict) -> dict:
        self.logger.info("=" * 50)
        self.logger.info("ШАГ 1: ПЛАНИРОВАНИЕ ЗАГРУЗКИ")
        self.logger.info("=" * 50)
        self.logger.info(
            "Заказ: %s x%d, дедлайн=%s, приоритет=%s",
            order["product"], order["quantity"],
            order["deadline"], order["priority"],
        )

        result = await self.send_task(
            "production.planning", "planning", order, timeout=15
        )

        if result.get("success"):
            output = json.loads(result["output"])
            self.logger.info(
                "Результат: feasible=%s, загрузка=%s%%",
                output.get("feasible"), output.get("utilization_pct"),
            )
            for s in output.get("schedule", []):
                self.logger.info(
                    "  %s: %s (%s - %s)",
                    s["machine"], s["task"], s["start_time"], s["end_time"],
                )
            if output.get("note"):
                self.logger.warning("ПРИМЕЧАНИЕ: %s", output["note"])
        else:
            self.logger.error("Ошибка планирования")
        return result

    async def check_inventory(self, material: str, required_qty: int) -> dict:
        self.logger.info("=" * 50)
        self.logger.info("ШАГ 2: ПРОВЕРКА ЗАПАСОВ")
        self.logger.info("=" * 50)
        self.logger.info("Материал: %s, требуется: %d", material, required_qty)

        result = await self.send_task(
            "production.inventory", "inventory_check",
            {"request_type": "check", "material": material,
             "required_qty": required_qty},
            timeout=10,
        )

        if result.get("success"):
            output = json.loads(result["output"])
            self.logger.info(
                "Результат: status=%s, доступно=%s, зарезервировано=%s",
                output.get("status"), output.get("available_qty"),
                output.get("reserved_qty"),
            )
            if output.get("note"):
                self.logger.info("ПРИМЕЧАНИЕ: %s", output["note"])
        return result

    async def reserve_materials(self, material: str, required_qty: int) -> dict:
        self.logger.info("=" * 50)
        self.logger.info("ШАГ 3: РЕЗЕРВ МАТЕРИАЛОВ")
        self.logger.info("=" * 50)

        result = await self.send_task(
            "production.inventory", "inventory_reserve",
            {"request_type": "reserve", "material": material,
             "required_qty": required_qty},
            timeout=10,
        )

        if result.get("success"):
            output = json.loads(result["output"])
            self.logger.info(
                "Результат: status=%s, зарезервировано=%d",
                output.get("status"), output.get("reserved_qty"),
            )
            if output.get("note"):
                self.logger.info("ПРИМЕЧАНИЕ: %s", output["note"])
        return result

    async def dispatch_production(
        self, order_id: str, schedule: list, priority: str
    ) -> dict:
        self.logger.info("=" * 50)
        self.logger.info("ШАГ 4: ДИСПЕТЧЕРИЗАЦИЯ")
        self.logger.info("=" * 50)
        self.logger.info("Заказ: %s, приоритет: %s", order_id, priority)

        result = await self.send_task(
            "production.dispatch", "dispatch",
            {
                "order_id": order_id,
                "schedule": schedule or [],
                "priority": priority,
                "start_after": datetime.now().strftime("%H:%M %d.%m.%Y"),
            },
            timeout=15,
        )

        if result.get("success"):
            output = json.loads(result["output"])
            self.logger.info(
                "Результат: dispatch_id=%s, status=%s",
                output.get("dispatch_id"), output.get("overall_status"),
            )
            for line in output.get("lines", []):
                self.logger.info(
                    "  %s: %s [%s]", line["line"], line["task"], line["status"],
                )
        return result

    async def quality_control(
        self, batch_id: str, product: str, quantity: int
    ) -> dict:
        self.logger.info("=" * 50)
        self.logger.info("ШАГ 5: КОНТРОЛЬ КАЧЕСТВА")
        self.logger.info("=" * 50)
        self.logger.info("Партия: %s, продукт: %s, объём: %d",
                          batch_id, product, quantity)

        result = await self.send_task(
            "production.quality", "quality_check",
            {
                "batch_id": batch_id,
                "product": product,
                "quantity": quantity,
                "measurements": [],
            },
            timeout=15,
        )

        if result.get("success"):
            output = json.loads(result["output"])
            self.logger.info(
                "Результат: status=%s, брак=%s%%, годных=%s%%",
                output.get("status"), output.get("defect_rate"),
                output.get("passed_pct"),
            )
            for defect in output.get("defects", []):
                self.logger.warning(
                    "  ДЕФЕКТ: %s (отклонение %s%%, %s)",
                    defect["param"], defect["deviation_pct"], defect["severity"],
                )
            if output.get("rework_note"):
                self.logger.warning("ПРИМЕЧАНИЕ: %s", output["rework_note"])
            if output.get("reject_note"):
                self.logger.error("ПРИМЕЧАНИЕ: %s", output["reject_note"])
        return result

    async def run_production_cycle(self, order: dict) -> dict:
        self.logger.info("#" * 60)
        self.logger.info("ЗАПУСК ПРОИЗВОДСТВЕННОГО ЦИКЛА")
        self.logger.info("Заказ: %s", order.get("order_id"))
        self.logger.info("#" * 60)

        schedule_data = None
        step1 = await self.plan_production(order)
        if step1.get("success"):
            out1 = json.loads(step1["output"])
            schedule_data = [
                {"machine": s["machine"], "task": s["task"], "duration_mins": 120}
                for s in out1.get("schedule", [])
            ]

        steel = await self.check_inventory("steel_1018", order["quantity"] * 2)
        reserve = await self.reserve_materials("steel_1018", order["quantity"] * 2)
        dispatch = await self.dispatch_production(
            order["order_id"], schedule_data or [], order["priority"]
        )

        await asyncio.sleep(1)

        qc = await self.quality_control(
            f"BATCH-{order['order_id']}", order["product"], order["quantity"]
        )

        self.logger.info("#" * 60)
        self.logger.info("ЦИКЛ ЗАВЕРШЁН")
        self.logger.info(self.metrics.report())
        self.logger.info("#" * 60)

        return {
            "planning": step1,
            "inventory_check": steel,
            "inventory_reserve": reserve,
            "dispatch": dispatch,
            "quality_control": qc,
        }

    async def demo_mode(self):
        self.logger.info("ДЕМО-РЕЖИМ: без NATS")
        agents = ["load_planner", "quality_control", "inventory", "dispatcher"]
        for a in agents:
            self.logger.info("  Имитация агента: %s", a)

        order = {
            "order_id": "ORD-2026-001",
            "product": "Шестерня",
            "quantity": 500,
            "deadline": (datetime.now() + timedelta(hours=8)).strftime(
                "%H:%M %d.%m.%Y"
            ),
            "priority": "high",
        }

        self.logger.info("#" * 60)
        self.logger.info("ДЕМО-ЗАКАЗ: %s", order)
        self.logger.info("#" * 60)

        self.logger.info("--- ШАГ 1: Планирование загрузки ---")
        self.logger.info("  Отправка задачи на production.planning...")
        self.logger.info("  Получен результат: feasible=True, загрузка=72.5%%")
        self.logger.info("    CNC-1: 250 ед. -> Line-A")
        self.logger.info("    CNC-2: 250 ед. -> Line-B")

        self.logger.info("--- ШАГ 2: Проверка запасов ---")
        self.logger.info("  Отправка задачи на production.inventory...")
        self.logger.info("  Получен результат: status=ok, available=5000")

        self.logger.info("--- ШАГ 3: Резерв материалов ---")
        self.logger.info("  Отправка задачи на production.inventory...")
        self.logger.info("  Получен результат: status=ok, reserved=1200")

        self.logger.info("--- ШАГ 4: Диспетчеризация ---")
        self.logger.info("  Отправка задачи на production.dispatch...")
        self.logger.info("  Получен результат: dispatch_id=DSP-20260522-1")

        self.logger.info("--- ШАГ 5: Контроль качества ---")
        self.logger.info("  Отправка задачи на production.quality...")
        self.logger.info("  Получен результат: status=passed, брак=1.2%%")

        self.logger.info("#" * 60)
        self.logger.info("ДЕМО-ЦИКЛ ЗАВЕРШЁН")
        self.logger.info("#" * 60)


async def main():
    logger = setup_logging()
    logger.info("Запуск оркестратора...")

    orchestrator = ProductionOrchestrator(logger=logger)

    if "--demo" in sys.argv:
        await orchestrator.demo_mode()
        return

    try:
        await orchestrator.connect()
    except Exception as e:
        logger.error("Не удалось подключиться к NATS: %s", e)
        logger.info("Используйте флаг --demo для демо-режима без NATS")
        return

    order = {
        "order_id": "ORD-2026-001",
        "product": "Шестерня",
        "quantity": 500,
        "deadline": (datetime.now() + timedelta(hours=8)).strftime(
            "%H:%M %d.%m.%Y"
        ),
        "priority": "high",
    }

    try:
        results = await orchestrator.run_production_cycle(order)
        logger.info("Итоговые результаты по шагам:")
        for step_name, result in results.items():
            if result.get("success"):
                out = json.loads(result.get("output", "{}"))
                logger.info(
                    "  %s: OK -> %s",
                    step_name,
                    json.dumps(out, ensure_ascii=False)[:100],
                )
            else:
                logger.error("  %s: FAIL", step_name)

        logger.info(orchestrator.metrics.report())
    except KeyboardInterrupt:
        logger.warning("Прервано пользователем")
    finally:
        await orchestrator.disconnect()
        logger.info("Оркестратор завершил работу")


if __name__ == "__main__":
    asyncio.run(main())
