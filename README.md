# Лабораторная работа №12. Мультиагентные системы: разработка распределённых интеллектуальных агентов
**Студент:** *Платов Артем Русланович*\
**Группа:** *220032-11*\
**Вариант:** *16*\
**Сложность:** *Средняя*
---
# Управление производством

## Архитектура

```
┌─────────────────────────────────────────────────────────┐
│                    PYTHON ORCHESTRATOR                   │
│  Отправляет задачи, собирает результаты, координирует    │
│  5-шаговый производственный workflow                     │
└──────┬──────────┬──────────┬──────────┬──────────────────┘
       │          │          │          │
       ▼          ▼          ▼          ▼
┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐
│ Planning │ Quality   │ Inventory │ Dispatch │
│ Load     │ Control   │ Manager   │ Agent    │
│ Agent    │ Agent     │ Agent     │          │
│ (Go)     │ (Go)      │ (Go)      │ (Go)     │
└──────────┘ └──────────┘ └──────────┘ └──────────┘
       │          │          │          │
       └──────────┴──────────┴──────────┘
                          │
                    ┌─────▼─────┐
                    │NATS Broker│
                    └───────────┘
```

### Компоненты

| Компонент | Язык | Роль |
|---|---|---|
| **Оркестратор** | Python 3.13+ | Центральный управляющий компонент: отправляет задачи, ожидает результаты, обрабатывает таймауты, выполняет 5-шаговый workflow |
| **Planning Agent** | Go 1.24+ | Планирование загрузки производственных мощностей |
| **Quality Control Agent** | Go 1.24+ | Контроль качества выпущенной продукции |
| **Inventory Agent** | Go 1.24+ | Управление складскими запасами |
| **Dispatcher Agent** | Go 1.24+ | Диспетчеризация заданий по производственным линиям |
| **NATS** | Docker | Брокер сообщений (асинхронная коммуникация) |

### Агенты и их бизнес-логика

#### 1. Планирование загрузки (`load_planner`)
- **Вход**: `{ order_id, product, quantity, deadline, priority }`
- **Выход**: `{ order_id, feasible, schedule: [{ machine, start_time, end_time, task }], total_time_mins, utilization_pct }`
- **Правила**:
  - Приоритет влияет на скорость обработки (critical ×2.0, high ×1.5, normal ×1.0, low ×0.5)
  - Загрузка станка не более 100%
  - Технологический перерыв 15 мин между операциями
  - Если deadline нереалистичен → `feasible: false` с рекомендацией
  - Critical-заказы выполняются даже при превышении дедлайна

#### 2. Контроль качества (`quality_control`)
- **Вход**: `{ batch_id, product, quantity, measurements: [{ param, value, nominal, unit, critical }] }`
- **Выход**: `{ batch_id, status, defect_rate, passed_pct, defects, rework_note, reject_note }`
- **Правила**:
  - Допуск: ±5% для обычных, ±1% для критических параметров
  - Если `defect_rate > 10%` или есть critical-дефект → `rejected`
  - Если `3% < defect_rate ≤ 10%` → `rework`
  - Выборочный контроль: 20% от партии, но не менее 10 единиц
  - Если измерения не переданы — генерируются автоматически

#### 3. Управление запасами (`inventory`)
- **Вход**: `{ request_type: "check"|"reserve"|"restock", material, required_qty, warehouse_id }`
- **Выход**: `{ request_id, status, available_qty, reserved_qty, safety_stock, estimated_arrival }`
- **Правила**:
  - Страховой запас: 15% от среднемесячного расхода
  - При дефиците автоматически оформляется заказ поставщику
  - Время поставки зависит от материала (2–14 дней)
  - Три режима: check (проверка), reserve (резервирование), restock (пополнение)

#### 4. Диспетчеризация (`dispatcher`)
- **Вход**: `{ order_id, schedule: [{ machine, task, duration_mins }], priority, start_after }`
- **Выход**: `{ order_id, dispatch_id, lines: [{ line, task, status, actual_start, expected_end }], overall_status }`
- **Правила**:
  - Задание назначается на линию с наименьшей загрузкой
  - Одна линия — одно задание за раз
  - Если линия занята — задание ставится в очередь
  - Critical/high приоритет может прерывать выполнение обычных заказов
  - При пустом расписании генерируется расписание по умолчанию

### Производственный workflow (оркестрация)

```
ШАГ 1: production.planning   → Планирование загрузки
ШАГ 2: production.inventory  → Проверка запасов
ШАГ 3: production.inventory  → Резерв материалов
ШАГ 4: production.dispatch   → Диспетчеризация
ШАГ 5: production.quality    → Контроль качества
```

---

## Установка и запуск

### Требования

- [Go](https://go.dev/dl/) 1.24+
- [Python](https://www.python.org/downloads/) 3.13+
- [Docker Desktop](https://www.docker.com/products/docker-desktop/)
- [Task](https://taskfile.dev/) (опционально, для автоматизации)

### 1. Клонирование

```bash
git clone <repository-url> lab5
cd lab5
```

### 2. Установка зависимостей

```bash
# Go-зависимости (агенты)
go mod download

# Python-зависимости (оркестратор)
pip install -r orchestrator/requirements.txt
```

### 3. Запуск NATS

```bash
docker compose up -d
```

Проверка: `curl http://localhost:8222` — мониторинг NATS.

### 4. Запуск агентов

В отдельных терминалах (или фоновых процессах):

```bash
# Терминал 1: Планирование загрузки
go run ./agents/cmd/load_planner/

# Терминал 2: Контроль качества
go run ./agents/cmd/quality_control/

# Терминал 3: Управление запасами
go run ./agents/cmd/inventory/

# Терминал 4: Диспетчеризация
go run ./agents/cmd/dispatcher/
```

Или соберите и запустите бинарники:

```bash
go build -o agents/bin/ ./agents/cmd/...
./agents/bin/load_planner.exe &
./agents/bin/quality_control.exe &
./agents/bin/inventory.exe &
./agents/bin/dispatcher.exe &
```

### 5. Запуск оркестратора

```bash
# Полный производственный цикл (с NATS и агентами)
python orchestrator/orchestrator.py

# Демо-режим (без NATS)
python orchestrator/orchestrator.py --demo
```

---

## Коммуникация (NATS Subjects)

| Subject | Направление | Назначение |
|---|---|---|
| `production.planning` | Оркестратор → Агент | Задачи планирования загрузки |
| `production.quality` | Оркестратор → Агент | Задачи контроля качества |
| `production.inventory` | Оркестратор → Агент | Задачи управления запасами |
| `production.dispatch` | Оркестратор → Агент | Задачи диспетчеризации |
| `production.completed` | Агент → Оркестратор | Результаты от всех агентов |

### Формат сообщений

**Задача (Task)**:
```json
{
  "id": "uuid",
  "type": "planning",
  "payload": "{...}"  // JSON-строка с данными конкретного агента
}
```

**Результат (Result)**:
```json
{
  "task_id": "uuid",
  "success": true,
  "output": "{...}",  // JSON-строка с результатом
  "agent": "load_planner"
}
```

---

## Тестирование

### Go-тесты агентов (37 тестов)

```bash
# Все тесты
go test ./agents/... -v

# Отдельный агент
go test ./agents/cmd/load_planner/ -v
go test ./agents/cmd/quality_control/ -v
go test ./agents/cmd/inventory/ -v
go test ./agents/cmd/dispatcher/ -v
go test ./agents/shared/ -v
```

| Пакет | Тестов | Что покрывают |
|---|---|---|
| `shared` | 4 | JSON-сериализация Task/Result |
| `load_planner` | 7 | Приоритеты, дедлайны, граничные случаи |
| `quality_control` | 9 | Допуски, статусы (passed/rework/rejected), границы 0%/100% |
| `inventory` | 9 | Check/reserve/restock, неизвестные материалы, состояние склада |
| `dispatcher` | 8 | Расписания, очереди, прерывания, утилиты |

### Python-тесты оркестратора (13 тестов)

```bash
cd orchestrator
pip install -r requirements.txt
pytest test_orchestrator.py -v
```

| Что проверяют |
|---|
| Инициализация, методы send_task / _on_result |
| Таймауты (при отсутствии подписчика) |
| Корректность payload каждого из 5 шагов workflow |
| Устойчивость к частичным отказам |
| Параллельные задачи |
| Ошибка подключения к NATS |

### Интеграционный тест (NATS e2e)

```bash
# Требует: запущенный NATS + собранные агенты
python test_integration.py
```

Проверяет сквозную коммуникацию: все 4 агента получают задачи через NATS и возвращают корректные ответы.

---

## Структура проекта

```
lab5/
├── docker-compose.yml              # NATS-сервер
├── go.mod / go.sum                 # Go-модуль (lab5)
├── README.md                       # Документация
├── agents/
│   ├── shared/
│   │   ├── types.go                # Task / Result — общий контракт
│   │   └── types_test.go           # Тесты shared-типов
│   ├── cmd/
│   │   ├── load_planner/
│   │   │   ├── main.go             # Агент планирования загрузки
│   │   │   └── main_test.go        # Тесты
│   │   ├── quality_control/
│   │   │   ├── main.go             # Агент контроля качества
│   │   │   └── main_test.go        # Тесты
│   │   ├── inventory/
│   │   │   ├── main.go             # Агент управления запасами
│   │   │   └── main_test.go        # Тесты
│   │   └── dispatcher/
│   │       ├── main.go             # Агент диспетчеризации
│   │       └── main_test.go        # Тесты
│   └── bin/                        # Скомпилированные бинарники
├── orchestrator/
│   ├── requirements.txt            # Зависимости Python
│   ├── orchestrator.py             # Оркестратор
│   └── test_orchestrator.py        # Тесты оркестратора
└── test_integration.py             # Интеграционный E2E-тест
```

---

## Taskfile (опционально)

Для автоматизации можно использовать [Taskfile](https://taskfile.dev/). Создайте `Taskfile.yml`:

```yaml
version: "3"

tasks:
  deps:
    desc: "Установка зависимостей"
    cmds:
      - go mod download
      - pip install -r orchestrator/requirements.txt

  build:
    desc: "Сборка агентов"
    cmds:
      - go build -o agents/bin/ ./agents/cmd/...

  up:
    desc: "Запуск NATS"
    cmds:
      - docker compose up -d

  down:
    desc: "Остановка NATS"
    cmds:
      - docker compose down

  test:
    desc: "Запуск всех тестов"
    cmds:
      - go test ./agents/... -v
      - pytest orchestrator/test_orchestrator.py -v

  run-orchestrator:
    desc: "Запуск оркестратора"
    cmds:
      - python orchestrator/orchestrator.py

  run-demo:
    desc: "Демо-режим оркестратора"
    cmds:
      - python orchestrator/orchestrator.py --demo
```

---

## Стек технологий

- **Go 1.24** — реализация агентов (конкурентность, NATS-клиент)
- **Python 3.13+** — оркестратор (asyncio, nats-py)
- **NATS** — асинхронный брокер сообщений
- **Docker** — контейнеризация NATS
- **pytest + asyncio** — тестирование Python
- **Go testing** — тестирование Go
