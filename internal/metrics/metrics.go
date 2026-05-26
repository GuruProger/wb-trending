package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Business метрики

// EventsProcessedTotal считает все входящие поисковые события с разбивкой по статусам.
// status: accepted | blocked_stoplist | blocked_antibot | outdated
// Ключевая метрика для понимания, какой процент трафика мы реально учитываем.
var EventsProcessedTotal = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "trending",
		Name:      "events_processed_total",
		Help:      "Total number of incoming search events by processing status",
	},
	[]string{"status"},
)

// AntiBotBlockedTotal считает запросы, отфильтрованные анти-ботом.
// Отделена от EventsProcessedTotal для удобства построения алертов
// (например, "если заблокировано > 50% за минуту - идет атака").
var AntiBotBlockedTotal = promauto.NewCounter(
	prometheus.CounterOpts{
		Namespace: "trending",
		Name:      "antibot_blocked_total",
		Help:      "Total number of requests blocked by anti-bot",
	},
)

// Performance метрики

// APIRequestDuration измеряет латенсию HTTP-запросов.
// Границы бакетов от 500мкс до 500мс покрывают типичный диапазон in-memory операций.
var APIRequestDuration = promauto.NewHistogramVec(
	prometheus.HistogramOpts{
		Namespace: "trending",
		Name:      "api_request_duration_seconds",
		Help:      "HTTP request duration in seconds",
		Buckets:   []float64{0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5},
	},
	[]string{"method", "path", "status"},
)

// APIRequestsTotal считает количество HTTP-запросов к API.
// Нужно для анализа соотношения read/write нагрузки.
var APIRequestsTotal = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "trending",
		Name:      "api_requests_total",
		Help:      "Total number of HTTP requests to API",
	},
	[]string{"method", "path", "status"},
)

// ConsumerProcessingDuration измеряет время обработки одного Kafka-сообщения.
// Показывает, насколько тяжелая операция storage.Add().
// Если p99 начнет расти - значит структура данных деградирует с ростом объема.
var ConsumerProcessingDuration = promauto.NewHistogram(
	prometheus.HistogramOpts{
		Namespace: "trending",
		Name:      "consumer_processing_duration_seconds",
		Help:      "Time to process a single Kafka message",
		Buckets:   []float64{0.00001, 0.00005, 0.0001, 0.0005, 0.001, 0.005, 0.01},
	},
)

// KafkaMessageLag показывает разницу между timestamp события и временем обработки.
// Если метрика растет - consumer не успевает за реальным временем.
var KafkaMessageLag = promauto.NewHistogram(
	prometheus.HistogramOpts{
		Namespace: "trending",
		Name:      "kafka_message_lag_seconds",
		Help:      "Time difference between event timestamp and processing time",
		Buckets:   []float64{0.01, 0.05, 0.1, 0.5, 1, 5, 10, 30, 60},
	},
)

// CacheRebuildDuration измеряет время пересборки кэша топа.
// Самая тяжелая операция в системе (snapshot + sort).
// Если p99 > 10мс - нужно увеличивать cacheTTL или оптимизировать сортировку.
var CacheRebuildDuration = promauto.NewHistogram(
	prometheus.HistogramOpts{
		Namespace: "trending",
		Name:      "cache_rebuild_duration_seconds",
		Help:      "Time to rebuild the top queries cache",
		Buckets:   []float64{0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1},
	},
)

// Resource метрики

// UniqueQueriesCount отражает количество уникальных запросов в скользящем окне.
var UniqueQueriesCount = promauto.NewGauge(
	prometheus.GaugeOpts{
		Namespace: "trending",
		Name:      "storage_unique_queries",
		Help:      "Number of unique queries currently in the sliding window",
	},
)

// TotalEventsCount показывает суммарное количество событий в окне (сумма всех count).
// Важно для оценки объема данных в памяти, а не только разнообразия запросов.
var TotalEventsCount = promauto.NewGauge(
	prometheus.GaugeOpts{
		Namespace: "trending",
		Name:      "storage_total_events",
		Help:      "Total number of events currently in the sliding window",
	},
)

// AntiBotCacheSize показывает текущий размер LRU-кэша анти-бота.
var AntiBotCacheSize = promauto.NewGauge(
	prometheus.GaugeOpts{
		Namespace: "trending",
		Name:      "antibot_cache_size",
		Help:      "Current number of entries in anti-bot LRU cache",
	},
)

// StopListSize отражает количество слов в стоп-листе.
var StopListSize = promauto.NewGauge(
	prometheus.GaugeOpts{
		Namespace: "trending",
		Name:      "stoplist_size",
		Help:      "Current number of words in the stoplist",
	},
)
