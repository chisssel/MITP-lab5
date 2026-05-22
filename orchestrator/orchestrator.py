import asyncio
import json
import uuid
import random
from datetime import datetime, timedelta
from typing import Dict, Optional

import nats


class ProductionOrchestrator:
    def __init__(self):
        self.nc: Optional[nats.NATS] = None
        self.results: Dict[str, asyncio.Future] = {}
        self.subscription = None

    async def connect(self):
        self.nc = await nats.connect("nats://localhost:4222")
        self.subscription = await self.nc.subscribe(
            "production.completed", cb=self._on_result
        )
        print("[Orchestrator] Подключён к NATS, слушаю production.completed")

    async def disconnect(self):
        if self.subscription:
            await self.subscription.unsubscribe()
        if self.nc:
            await self.nc.drain()
            self.nc.close()

    async def _on_result(self, msg):
        data = json.loads(msg.data.decode())
        task_id = data.get("task_id")
        if task_id in self.results:
            future = self.results.pop(task_id)
            if not future.done():
                future.set_result(data)
            print(
                f"  [Orchestrator] Получен результат от {data.get('agent', '?')}: "
                f"success={data.get('success')}"
            )

    async def send_task(
        self, subject: str, task_type: str, payload: dict, timeout: int = 30
    ) -> dict:
        task_id = str(uuid.uuid4())
        task = {"id": task_id, "type": task_type, "payload": json.dumps(payload)}

        future = asyncio.Future()
        self.results[task_id] = future

        await self.nc.publish(subject, json.dumps(task).encode())
        print(f"  [Orchestrator] Отправлена задача {task_id} типа '{task_type}' -> {subject}")

        try:
            result = await asyncio.wait_for(future, timeout)
            return result
        except asyncio.TimeoutError:
            self.results.pop(task_id, None)
            print(f"  [Orchestrator] ТАЙМАУТ: задача {task_id} не выполнена за {timeout} сек")
            return {"task_id": task_id, "success": False, "output": "{}", "agent": "timeout"}

    async def plan_production(self, order: dict) -> dict:
        print(f"\n{'='*60}")
        print(" ШАГ 1: ПЛАНИРОВАНИЕ ЗАГРУЗКИ")
        print(f"{'='*60}")
        print(f"  Заказ: {order['product']} x{order['quantity']}, "
              f"дедлайн={order['deadline']}, приоритет={order['priority']}")

        result = await self.send_task(
            "production.planning", "planning", order, timeout=15
        )

        if result.get("success"):
            output = json.loads(result["output"])
            print(f"  Результат: feasible={output.get('feasible')}, "
                  f"загрузка={output.get('utilization_pct')}%")
            if output.get("schedule"):
                for s in output["schedule"]:
                    print(f"    - {s['machine']}: {s['task']} ({s['start_time']} - {s['end_time']})")
            if output.get("note"):
                print(f"  ПРИМЕЧАНИЕ: {output['note']}")
        else:
            print(f"  Ошибка планирования")
        return result

    async def check_inventory(self, material: str, required_qty: int) -> dict:
        print(f"\n{'='*60}")
        print(" ШАГ 2: ПРОВЕРКА ЗАПАСОВ")
        print(f"{'='*60}")
        print(f"  Материал: {material}, требуется: {required_qty}")

        result = await self.send_task(
            "production.inventory", "inventory_check",
            {"request_type": "check", "material": material, "required_qty": required_qty},
            timeout=10
        )

        if result.get("success"):
            output = json.loads(result["output"])
            print(f"  Результат: status={output.get('status')}, "
                  f"доступно={output.get('available_qty')}, "
                  f"зарезервировано={output.get('reserved_qty')}")
            if output.get("note"):
                print(f"  ПРИМЕЧАНИЕ: {output['note']}")
        return result

    async def reserve_materials(self, material: str, required_qty: int) -> dict:
        print(f"\n{'='*60}")
        print(" ШАГ 3: РЕЗЕРВ МАТЕРИАЛОВ")
        print(f"{'='*60}")

        result = await self.send_task(
            "production.inventory", "inventory_reserve",
            {"request_type": "reserve", "material": material, "required_qty": required_qty},
            timeout=10
        )

        if result.get("success"):
            output = json.loads(result["output"])
            print(f"  Результат: status={output.get('status')}, "
                  f"зарезервировано={output.get('reserved_qty')}")
            if output.get("note"):
                print(f"  ПРИМЕЧАНИЕ: {output['note']}")
        return result

    async def dispatch_production(self, order_id: str, schedule: list, priority: str) -> dict:
        print(f"\n{'='*60}")
        print(" ШАГ 4: ДИСПЕТЧЕРИЗАЦИЯ")
        print(f"{'='*60}")
        print(f"  Заказ: {order_id}, приоритет: {priority}")

        result = await self.send_task(
            "production.dispatch", "dispatch",
            {
                "order_id": order_id,
                "schedule": schedule or [],
                "priority": priority,
                "start_after": datetime.now().strftime("%H:%M %d.%m.%Y"),
            },
            timeout=15
        )

        if result.get("success"):
            output = json.loads(result["output"])
            print(f"  Результат: dispatch_id={output.get('dispatch_id')}, "
                  f"status={output.get('overall_status')}")
            for line in output.get("lines", []):
                print(f"    - {line['line']}: {line['task']} [{line['status']}]")
        return result

    async def quality_control(self, batch_id: str, product: str, quantity: int) -> dict:
        print(f"\n{'='*60}")
        print(" ШАГ 5: КОНТРОЛЬ КАЧЕСТВА")
        print(f"{'='*60}")
        print(f"  Партия: {batch_id}, продукт: {product}, объём: {quantity}")

        result = await self.send_task(
            "production.quality", "quality_check",
            {
                "batch_id": batch_id,
                "product": product,
                "quantity": quantity,
                "measurements": [],
            },
            timeout=15
        )

        if result.get("success"):
            output = json.loads(result["output"])
            print(f"  Результат: status={output.get('status')}, "
                  f"брак={output.get('defect_rate')}%, "
                  f"годных={output.get('passed_pct')}%")
            for defect in output.get("defects", []):
                print(f"    - ДЕФЕКТ: {defect['param']} ({defect['deviation']}%, {defect['severity']})")
            if output.get("rework_note"):
                print(f"  ПРИМЕЧАНИЕ: {output['rework_note']}")
            if output.get("reject_note"):
                print(f"  ПРИМЕЧАНИЕ: {output['reject_note']}")
        return result

    async def run_production_cycle(self, order: dict):
        print(f"\n{'#'*60}")
        print(f" ЗАПУСК ПРОИЗВОДСТВЕННОГО ЦИКЛА")
        print(f" Заказ: {order.get('order_id')}")
        print(f"{'#'*60}")

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

        print(f"\n{'#'*60}")
        print(" ЦИКЛ ЗАВЕРШЁН")
        print(f"{'#'*60}")
        return {
            "planning": step1,
            "inventory_check": steel,
            "inventory_reserve": reserve,
            "dispatch": dispatch,
            "quality_control": qc,
        }

    async def demo_mode(self):
        print("[Orchestrator] ДЕМО-РЕЖИМ: без NATS")
        agents = ["load_planner", "quality_control", "inventory", "dispatcher"]
        for a in agents:
            print(f"  Имитация агента: {a}")

        order = {
            "order_id": "ORD-2026-001",
            "product": "Шестерня",
            "quantity": 500,
            "deadline": (datetime.now() + timedelta(hours=8)).strftime("%H:%M %d.%m.%Y"),
            "priority": "high",
        }

        print(f"\n{'#'*60}")
        print(f" ДЕМО-ЗАКАЗ: {order}")
        print(f"{'#'*60}")

        print(f"\n--- ШАГ 1: Планирование загрузки ---")
        print(f"  Отправка задачи на production.planning...")
        print(f"  Получен результат: feasible=True, загрузка=72.5%")
        print(f"    CNC-1: 250 ед. -> Line-A")
        print(f"    CNC-2: 250 ед. -> Line-B")

        print(f"\n--- ШАГ 2: Проверка запасов ---")
        print(f"  Отправка задачи на production.inventory...")
        print(f"  Получен результат: status=ok, available=5000, reserved=200")

        print(f"\n--- ШАГ 3: Резерв материалов ---")
        print(f"  Отправка задачи на production.inventory...")
        print(f"  Получен результат: status=ok, reserved=1200")

        print(f"\n--- ШАГ 4: Диспетчеризация ---")
        print(f"  Отправка задачи на production.dispatch...")
        print(f"  Получен результат: dispatch_id=DSP-20260522-1, status=in_progress")
        print(f"    Line-A: Обработка -> in_progress")
        print(f"    Line-B: Сборка -> in_progress")

        print(f"\n--- ШАГ 5: Контроль качества ---")
        print(f"  Отправка задачи на production.quality...")
        print(f"  Получен результат: status=passed, брак=1.2%, годных=98.8%")

        print(f"\n{'#'*60}")
        print(" ДЕМО-ЦИКЛ ЗАВЕРШЁН")
        print(f"{'#'*60}")


async def main():
    import sys

    orchestrator = ProductionOrchestrator()

    if "--demo" in sys.argv:
        await orchestrator.demo_mode()
        return

    try:
        await orchestrator.connect()
    except Exception as e:
        print(f"[Orchestrator] Не удалось подключиться к NATS: {e}")
        print("[Orchestrator] Используйте флаг --demo для демо-режима без NATS")
        return

    order = {
        "order_id": "ORD-2026-001",
        "product": "Шестерня",
        "quantity": 500,
        "deadline": (datetime.now() + timedelta(hours=8)).strftime("%H:%M %d.%m.%Y"),
        "priority": "high",
    }

    try:
        results = await orchestrator.run_production_cycle(order)
        print("\nИтоговые результаты по шагам:")
        for step_name, result in results.items():
            if result.get("success"):
                out = json.loads(result.get("output", "{}"))
                print(f"  {step_name}: OK -> {json.dumps(out, ensure_ascii=False)[:100]}")
            else:
                print(f"  {step_name}: FAIL")
    except KeyboardInterrupt:
        print("\n[Orchestrator] Прервано пользователем")
    finally:
        await orchestrator.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
