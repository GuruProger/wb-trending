
## 📋 Содержание

- [🎯 Задача](#-задача)
- [🚀 Быстрый старт](#-быстрый-старт)
- [⚙️ Конфигурация](#️-конфигурация)
- [📁 Структура проекта](#-структура-проекта)
- [🏗 Архитектура](#-архитектура)
- [📦 Контракт данных (payload)](#-контракт-данных-payload)
- [🔌 API](#-api)
- [🎭 Бизнес-логика](#-бизнес-логика)
- [⚖️ Trade-offs и обоснование решений](#️-trade-offs-и-обоснование-решений)
- [⚡ Производительность](#-производительность)
- [📊 Мониторинг](#-мониторинг)
- [🧪 Тестирование](#-тестирование)

---

## 🎯 Задача

Спроектировать и реализовать in-memory сервис, который:
- Читает поток поисковых запросов из брокера сообщений (Kafka)
- Хранит скользящее окно событий за последние **5 минут**
- Отдаёт топ-N запросов с минимальной задержкой (p99 < 5ms)
- Выдерживает нагрузку, где **чтений в 10–50 раз больше, чем записей**
- Фильтрует накрутки от конкурентов/парсеров
- Позволяет маркетологам управлять стоп-листом «на лету»

**Ключевой вызов:** горячий путь должен работать исключительно в оперативной памяти без обращения к внешним БД, иначе виджет на главной странице не справится с нагрузкой.

---

## 🚀 Быстрый старт

### Требования
- Go 1.26
- Docker & Docker Compose

### Запуск одной командой

```bash
# 1. Поднимаем инфраструктуру (Kafka, Prometheus, Grafana)
docker compose up -d

# 2. Запускаем сервис
go run cmd/server/main.go

# 3. В отдельном терминале — отправляем тестовые события
go run cmd/producer/main.go

# 4. Проверяем результат
curl "http://localhost:8080/api/v1/trending?limit=10"
```

### Доступные сервисы

| Сервис | URL | Логин/Пароль |
|--------|-----|--------------|
| API | http://localhost:8080 | — |
| Prometheus | http://localhost:9090 | — |
| Grafana | http://localhost:3000 | `admin` / `admin` |


---
## 📁 Структура проекта

```
wb-trending/
├── cmd/
│   ├── server/main.go              # Точка входа, graceful shutdown, /metrics
│   └── producer/main.go            # Mock-продюсер для тестирования
├── internal/
│   ├── api/
│   │   ├── handler.go              # Общая структура и регистрация роутов
│   │   ├── trending.go             # GET /api/v1/trending
│   │   ├── stoplist.go             # CRUD /api/v1/stoplist/
│   │   └── metrics_middleware.go   # Prometheus middleware для HTTP
│   ├── config/config.go            # Конфигурация
│   ├── consumer/kafka.go           # Kafka consumer с метриками
│   ├── metrics/metrics.go          # Определения всех Prometheus-метрик
│   ├── models/event.go             # Контракт SearchEvent
│   └── storage/
│       ├── storage.go              # Интерфейс TrendingStorage
│       ├── ring_buffer.go          # Sliding window + global aggregator
│       ├── stoplist.go             # Динамический стоп-лист
│       ├── antibot.go              # Token Bucket + LRU
│       ├── *_test.go               # Unit-тесты
│       └── *_bench_test.go         # Бенчмарки
├── monitoring/
│   ├── prometheus.yml              # Конфиг Prometheus
│   └── grafana/provisioning/       # Auto-provisioned datasources & dashboards
├── docker-compose.yml              # Kafka KRaft + Prometheus + Grafana
├── go.mod
└── README.md
```

---

## 🏗 Архитектура

```mermaid
flowchart LR
    subgraph SearchService["Поисковый сервис"]
        S[Producer]
    end

    subgraph TrendingService["Trending Service"]
        K[Kafka Consumer] --> ST[Storage Layer]

        subgraph Storage Layer
            SL[Stop-List] -->|filter| ADD[Add]
            AB[Anti-Bot LRU] -->|filter| ADD
            ADD --> RB[Ring Buffer<br/>300 buckets × 1s]
            ADD --> AG[Global Aggregator]
        end

        ST --> API[HTTP API /trending]
        API --> CACHE[(Cache TTL 500ms)]
    end

    subgraph Observability
        PM[Prometheus] -.scrape.-> M[/metrics]
        GF[Grafana] --> PM
    end

    S -->|search_events| K
    M -.expose.-> ST
```


### Основные компоненты

| Компонент | Назначение |
|-----------|------------|
| **Kafka Consumer** | Читает поток событий с батчингом и graceful shutdown |
| **Storage Layer** | In-memory хранилище со скользящим окном |
| **Ring Buffer** | 300 односекундных бакетов = 5-минутное окно |
| **Global Aggregator** | Precomputed счётчики для O(1) чтения топа |
| **Cache** | TTL 500ms, снижает нагрузку на rebuild |
| **Anti-Bot** | Token Bucket + LRU для борьбы с накрутками |
| **Stop-List** | Динамический фильтр нежелательных слов |

---

## 📦 Контракт данных (payload)

Формат сообщения в Kafka-топике `search_events`:

```json
{
  "query": "iphone 17",
  "timestamp": 1716800000000,
  "user_id": "usr_8a3f2b1c"
}
```

### Обоснование полей

| Поле | Тип | Зачем нужно |
|------|-----|-------------|
| `query` | `string` | Основной объект агрегации — сам поисковый запрос |
| `timestamp` | `int64` (Unix ms) | Позволяет корректно строить скользящее окно и обрабатывать **out-of-order** события из Kafka (partition rebalance, задержки сети). Не полагаемся на `kafka.Message.Time`, т.к. он отражает момент записи в топик, а не момент действия пользователя |
| `user_id` | `string` | Необходим для **анти-бота**: связка `(user_id, query)` позволяет отследить аномальную активность конкретного пользователя и отфильтровать накрутки. Без этого поля защититься от таргетированных атак невозможно |

### Почему **нет** `session_id` / `ip`?
Для MVP достаточно `user_id`. В production-версии можно добавить `session_id` для борьбы с неавторизованными ботами и `ip` для сетевых rate-limiter'ов, но это усложнит контракт и увеличит объём данных в Kafka.

---

## 🔌 API

### `GET /api/v1/trending?limit=N`
Возвращает топ-N популярных запросов за последние 5 минут.

**Параметры:**
- `limit` (query, optional) — количество результатов. По умолчанию `10`, максимум `100`.

**Пример запроса:**
```bash
curl "http://localhost:8080/api/v1/trending?limit=5"
```

**Пример ответа:**
```json
{
  "queries": ["iphone 17", "кроссовки nike", "ноутбук asus", "наушники airpods", "кроссовки adidas"],
  "count": 5
}
```

### `GET /api/v1/stoplist/`
Возвращает текущий стоп-лист.

**Пример запроса:**
```bash
curl http://localhost:8080/api/v1/stoplist/
```

**Пример ответа:**
```json
{
  "words": ["реклама", "спам"],
  "count": 2
}
```

### `POST /api/v1/stoplist/`
Добавляет слово в стоп-лист.

**Пример запроса:**
```bash
curl -X POST http://localhost:8080/api/v1/stoplist/ \
  -H "Content-Type: application/json" \
  -d '{"word": "нежелательное_слово"}'
```

**Пример ответа:**
```json
{
  "message": "Слово добавлено в стоп-лист",
  "word": "нежелательное_слово"
}
```

### `DELETE /api/v1/stoplist/`
Удаляет слово из стоп-листа.

**Пример запроса:**
```bash
curl -X DELETE http://localhost:8080/api/v1/stoplist/ \
  -H "Content-Type: application/json" \
  -d '{"word": "нежелательное_слово"}'
```

**Пример ответа:**
```json
{
  "message": "Слово удалено из стоп-листа",
  "word": "нежелательное_слово"
}
```

### `GET /health`
Health-check для liveness/readiness probes.

**Пример запроса:**
```bash
curl http://localhost:8080/health
```

**Ответ:** `OK`

### `GET /metrics`
Prometheus-метрики сервиса.

**Пример запроса:**
```bash
curl http://localhost:8080/metrics | grep trending_
```

---

## 🎭 Бизнес-логика

### 1. Стоп-лист (Stop-List)
Реализован как `map[string]bool` с `sync.RWMutex`. Фильтрация происходит **до** захвата мьютекса хранилища — это снижает lock contention на горячем пути.

```
SearchEvent → IsBlocked(query)? 
              ├─ Yes → drop + counter
              └─ No  → continue to storage
```

**Поведение:**
- Case-sensitive (как и поиск)
- Точное совпадение (без regex/подстрок) — для производительности
- Применяется ко всем входящим событиям, а не только к API

### 2. Анти-бот (Anti-Bot)
Реализован через алгоритм **Token Bucket** с персистентностью в **LRU-кэше с TTL**.

**Принцип работы:**
- На каждую пару `(user_id, query)` заводится корзина с `maxTokens=1` токеном
- Токен восстанавливается каждые `10 секунд`
- Если корзина пуста — запрос отфильтровывается
- LRU-кэш ограничен **100,000 записями** с TTL 5 минут

**Защита от:**
- ✅ Накрутки одного запроса одним пользователем
- ✅ Парсеров с ограниченным пулом ID
- ✅ DDoS-атак (LRU лимит защищает от OOM)

**Не защищает от:**
- ❌ Ботнета с миллионами уникальных `user_id` (тут нужен Query Velocity Limit — см. [Что можно улучшить](#-что-можно-улучшить))

---

## ⚖️ Trade-offs и обоснование решений

### 🔴 Критичные архитектурные решения

| Решение | Альтернативы | Почему выбрано именно это |
|---------|--------------|---------------------------|
| **In-memory без БД** | Redis, PostgreSQL, ClickHouse | Требование «максимальной скорости» чтения + «чтений в 10-50 раз больше» исключает поход в сеть. In-memory даёт ~100ns на чтение против ~1ms у Redis |
| **Ring Buffer с 1-секундной гранулярностью** | Linked list всех событий, Min-heap с таймерами | Жертвуем миллисекундной точностью ради O(1) очистки устаревших данных. 300 бакетов × средний размер ≈ 5MB памяти при любом трафике |
| **Global Aggregator (precomputed)** | Пересчёт топа на каждый запрос | `GetTop()` из кэша: **104 ns/op** vs ~400 μs при полном пересчёте. Платим памятью (map[string]int) за скорость чтения |
| **Cache TTL 500ms** | Без кэша / Write-through cache | При 10K RPS это снижает нагрузку на `rebuildCache()` в 5000 раз. 500ms — невидимая для пользователя задержка актуальности виджета |
| **Token Bucket вместо Fixed Window** | Fixed Window, Sliding Window Log | Token Bucket даёт плавную деградацию без «эффекта границы окна». LRU защищает от OOM при DDoS |
| **Kafka без Zookeeper (KRaft)** | Kafka + Zookeeper | Упрощает инфраструктуру: один контейнер вместо двух. KRaft — современный стандарт для Kafka 3.x+ |
| **chi вместо Gin/Echo** | Gin, Echo, Fiber | Идиоматичный, легковесный, использует только стандартную библиотеку `net/http`. Меньше магии — проще отладка |
| **segmentio/kafka-go без CGO** | confluent-kafka-go | Нет зависимости от librdkafka, проще деплой. Производительности хватает с запасом |

### 🟡 Выявленные проблемы в продуктовой постановке

1. **«Топ за последние 5 минут» + out-of-order события**
   - **Проблема:** Kafka не гарантирует порядок между партициями, а сетевые задержки могут приносить «старые» события
   - **Решение:** Храним `currentSecond` и корректно обрабатываем события из прошлого (в пределах окна) и отбрасываем слишком старые

2. **«Конкуренты генерируют аномальные всплески»**
   - **Проблема:** Неясно, по какому именно идентификатору детектировать аномалии
   - **Решение:** Ввели в контракт `user_id` + Token Bucket. Дополнительно можно добавить Query Velocity Limit (ограничение скорости поступления запроса глобально)

3. **«Виджет на главной странице» → холодный старт**
   - **Проблема:** При старте сервиса окно пустое — виджет будет показывать «пустоту» до 5 минут
   - **Решение:** Accept as is. В production можно добавить bootstrap из clickhouse/s3 для быстрого прогрева

4. **Стоп-лист не имеет версионности**
   - **Проблема:** При ошибочном удалении слова нельзя откатиться
   - **Решение:** Для MVP достаточно. В production нужен audit log + версионность (event sourcing)

---

## ⚡ Производительность

Измерения проводились на **Intel Core i5-9300HF @ 2.40GHz** через `go test -bench`.

### Unit Benchmarks

#### Ring Buffer (ядро системы)

| Benchmark | ns/op | ops/sec | Allocations |
|-----------|-------|---------|-------------|
| `Add (single)` | 436 | 2.3M | 1 alloc, 24 B |
| `Add (parallel, 8 cores)` | 661 | **1.5M** | 3 allocs, 50 B |
| `GetTop Cached (parallel, 8 cores)` | **104** | **9.6M** | 1 alloc, 160 B |
| `GetTop Rebuild` | 400,000 | 2.5K | 10 allocs, 79 KB |

#### AntiBot (Token Bucket + LRU)

| Benchmark | ns/op | ops/sec |
|-----------|-------|---------|
| `Allow (first request)` | 1,599 | 625K |
| `Allow (cached hit)` | 288 | 3.5M |
| `Allow (blocked)` | 286 | 3.5M |
| `Allow (parallel, 8 cores)` | 564 | 1.8M |

#### StopList

| Benchmark | ns/op | ops/sec |
|-----------|-------|---------|
| `IsBlocked (hit)` | 36 | 27.8M |
| `IsBlocked (miss)` | 37 | 27.0M |
| `IsBlocked (parallel, 8 cores)` | 80 | 12.5M |

### Ключевые выводы

1. **Чтение (`GetTop`) масштабируется линейно** по количеству ядер благодаря `sync.RWMutex`. Это критично для сценария «чтений в 50 раз больше записей».
2. **Запись (`Add`) не масштабируется** — bottleneck в глобальном `sync.Mutex`. Это компромисс ради простоты инкрементального агрегатора. 1.5M ops/sec на одном инстансе — избыточно для большинства сценариев (Kafka consumer обычно работает в 1 горутину на партицию).
3. **StopList практически бесплатный** — 36ns/op, что на порядок быстрее самой операции `Add`.
4. **`rebuildCache` тяжёлый (400μs), но редкий** — благодаря cacheTTL 500ms он происходит максимум 2 раза в секунду на инстанс, не влияя на общую латенсию.

---

## 📊 Мониторинг

Сервис экспортирует **11 метрик Prometheus**, сгруппированных по назначению.

### Business-метрики
- `trending_events_processed_total{status}` — статусы обработки: `accepted`, `blocked_stoplist`, `blocked_antibot`, `outdated`, `invalid_json`
- `trending_antibot_blocked_total` — счётчик отфильтрованных анти-ботом запросов

### Performance-метрики
- `trending_api_request_duration_seconds{method,path,status}` — гистограмма латенсии API (бакеты от 500мкс)
- `trending_api_requests_total{method,path,status}` — общее количество HTTP-запросов
- `trending_consumer_processing_duration_seconds` — время обработки одного Kafka-сообщения
- `trending_kafka_message_lag_seconds` — отставание от реального времени
- `trending_cache_rebuild_duration_seconds` — время самой тяжёлой операции

### Resource-метрики
- `trending_storage_unique_queries` — уникальных запросов в окне
- `trending_storage_total_events` — суммарное количество событий в окне
- `trending_antibot_cache_size` — размер LRU-кэша анти-бота
- `trending_stoplist_size` — размер стоп-листа

**Плюс автоматически:** Go runtime метрики (goroutines, memory, GC) через `promhttp`.

### Готовый Grafana-дашборд

Docker-compose автоматически поднимает Grafana с provisioned дашбордом **«WB Trending Service»**, содержащим 7 ключевых панелей:
- Events Processing Rate по статусам
- API Request Duration (p95, p99)
- Storage Metrics (unique queries, total events)
- Cache Rebuild Duration (p99)
- Kafka Message Lag (p95)
- Anti-Bot Cache Size & Stop List Size

---

## 🧪 Тестирование

```bash
# Unit-тесты
go test ./internal/storage/... -v

# С покрытием
go test ./internal/storage/... -cover

# Бенчмарки
go test -bench=. -benchmem ./internal/storage/... -v

# Параллельные бенчмарки на разных CPU
go test -bench=Parallel -benchmem -cpu=1,2,4,8 ./internal/storage/... -v
```

**Покрытие:**
- ✅ 8 тестов StopList (CRUD, concurrency, edge cases)
- ✅ 12 тестов AntiBot (Token Bucket logic, LRU eviction, stats)
- ✅ 12 тестов RingBuffer (sliding window, caching, integration)
- ✅ Все тесты включают сценарии concurrent access

