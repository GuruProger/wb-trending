package storage

import (
	"sort"
	"sync"
	"time"

	"github.com/GuruProger/wb-trending/internal/metrics"
	"github.com/GuruProger/wb-trending/internal/models"
)

type Bucket struct {
	Timestamp int64
	Queries   map[string]int
}

type RingBufferStorage struct {
	mu sync.RWMutex

	buckets     []*Bucket
	bucketCount int

	aggregated map[string]int

	totalEvents int64 // Сумма всех count в агрегаторе, для метрики TotalEventsCount

	currentSecond int64

	cachedTop       []cachedEntry
	cacheValidUntil time.Time
	cacheTTL        time.Duration

	// Стоп-лист для фильтрации нежелательных запросов
	stopList *StopList

	// Анти-бот для защиты от накруток
	antiBot *AntiBot
}

type cachedEntry struct {
	Query string
	Count int
}

func NewRingBufferStorage(windowSize, bucketSize time.Duration) *RingBufferStorage {
	if bucketSize != time.Second {
		bucketSize = time.Second
	}

	bucketCount := int(windowSize / bucketSize)
	buckets := make([]*Bucket, bucketCount)
	for i := range buckets {
		buckets[i] = &Bucket{
			Timestamp: -1,
			Queries:   make(map[string]int),
		}
	}

	return &RingBufferStorage{
		buckets:     buckets,
		bucketCount: bucketCount,
		aggregated:  make(map[string]int),
		cacheTTL:    500 * time.Millisecond,

		stopList: NewStopList(),
		// Кэш на 100000 пар, 1 токен, восстановление за 10 секунд
		antiBot: NewAntiBot(100000, 1, 10*time.Second),
	}
}

func (s *RingBufferStorage) Add(event models.SearchEvent) {
	// Проверяем стоп-лист перед добавлением
	if s.stopList.IsBlocked(event.Query) {
		metrics.EventsProcessedTotal.WithLabelValues("blocked_stoplist").Inc()
		return
	}

	// Проверяем анти-бот: если это накрутка, игнорируем запрос
	if !s.antiBot.Allow(event.UserID, event.Query) {
		metrics.EventsProcessedTotal.WithLabelValues("blocked_antibot").Inc()
		return
	}

	eventSecond := event.Timestamp / 1000

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.currentSecond == 0 {
		s.currentSecond = eventSecond
	}

	// Событие старше скользящего окна - игнорируем
	if eventSecond < s.currentSecond-int64(s.bucketCount) {
		metrics.EventsProcessedTotal.WithLabelValues("outdated").Inc()
		return
	}

	if eventSecond < s.currentSecond {
		s.incrementBucket(eventSecond, event.Query)
		metrics.EventsProcessedTotal.WithLabelValues("accepted").Inc()
		return
	}

	for sec := s.currentSecond + 1; sec <= eventSecond; sec++ {
		s.moveToNewBucket(sec)
	}
	s.currentSecond = eventSecond

	s.incrementBucket(eventSecond, event.Query)
	metrics.EventsProcessedTotal.WithLabelValues("accepted").Inc()
}

// GetStopList возвращает ссылку на стоп-лист для управления из API
func (s *RingBufferStorage) GetStopList() *StopList {
	return s.stopList
}

// incrementBucket добавляет событие в бакет и обновляет глобальный агрегатор.
// Вместе с этим обновляем gauge-метрики, чтобы они отражали актуальное состояние памяти.
func (s *RingBufferStorage) incrementBucket(second int64, query string) {
	idx := int(second % int64(s.bucketCount))
	bucket := s.buckets[idx]

	if bucket.Timestamp != second && bucket.Timestamp != -1 {
		s.clearBucket(bucket)
		bucket.Timestamp = second
		bucket.Queries = make(map[string]int)
	}

	if bucket.Timestamp == -1 {
		bucket.Timestamp = second
	}

	isNewQuery := s.aggregated[query] == 0

	bucket.Queries[query]++
	s.aggregated[query]++
	s.totalEvents++

	// Обновляем метрики только при появлении нового query в системе
	if isNewQuery {
		metrics.UniqueQueriesCount.Set(float64(len(s.aggregated)))
	}
	metrics.TotalEventsCount.Set(float64(s.totalEvents))
}

func (s *RingBufferStorage) moveToNewBucket(newSecond int64) {
	idx := int(newSecond % int64(s.bucketCount))
	bucket := s.buckets[idx]
	s.clearBucket(bucket)

	bucket.Timestamp = newSecond
	bucket.Queries = make(map[string]int)
}

// clearBucket вычитает старые значения из глобального счетчика перед повторным использованием интервала
func (s *RingBufferStorage) clearBucket(bucket *Bucket) {
	if bucket.Timestamp == -1 {
		return
	}

	for query, count := range bucket.Queries {
		newVal := s.aggregated[query] - count
		if newVal <= 0 {
			delete(s.aggregated, query)
		} else {
			s.aggregated[query] = newVal
		}
		s.totalEvents -= int64(count)
	}

	// Обновляем метрики после очистки - показываем реальное состояние памяти
	metrics.UniqueQueriesCount.Set(float64(len(s.aggregated)))
	metrics.TotalEventsCount.Set(float64(s.totalEvents))

	bucket.Timestamp = -1
}

func (s *RingBufferStorage) GetTop(limit int) []string {
	s.mu.RLock()
	if time.Now().Before(s.cacheValidUntil) && len(s.cachedTop) > 0 {
		result := s.copyTopLocked(limit)
		s.mu.RUnlock()
		return result
	}
	s.mu.RUnlock()

	return s.rebuildCache(limit)
}

func (s *RingBufferStorage) copyTopLocked(limit int) []string {
	n := limit
	if n > len(s.cachedTop) {
		n = len(s.cachedTop)
	}
	result := make([]string, n)
	for i := 0; i < n; i++ {
		result[i] = s.cachedTop[i].Query
	}
	return result
}

// rebuildCache - самая тяжелая операция в системе.
// Измеряем её длительность через гистограмму, чтобы вовремя заметить деградацию.
func (s *RingBufferStorage) rebuildCache(limit int) []string {
	start := time.Now()
	defer func() {
		metrics.CacheRebuildDuration.Observe(time.Since(start).Seconds())
	}()

	s.mu.Lock()

	// Очищаем устаревшие бакеты перед агрегацией
	currentTime := time.Now().Unix()
	cutoffTime := currentTime - int64(s.bucketCount)

	// Продвигаем currentSecond вперед, если прошло много времени без событий
	if currentTime > s.currentSecond {
		for sec := s.currentSecond + 1; sec <= currentTime; sec++ {
			if sec > cutoffTime {
				s.moveToNewBucket(sec)
			}
		}
		s.currentSecond = currentTime
	}

	snapshot := make(map[string]int, len(s.aggregated))
	for q, c := range s.aggregated {
		snapshot[q] = c
	}
	s.mu.Unlock()

	entries := make([]cachedEntry, 0, len(snapshot))
	for q, c := range snapshot {
		entries = append(entries, cachedEntry{Query: q, Count: c})
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].Count != entries[j].Count {
			return entries[i].Count > entries[j].Count
		}
		return entries[i].Query < entries[j].Query
	})

	s.mu.Lock()
	s.cachedTop = entries
	s.cacheValidUntil = time.Now().Add(s.cacheTTL)
	s.mu.Unlock()

	n := limit
	if n > len(entries) {
		n = len(entries)
	}
	result := make([]string, n)
	for i := 0; i < n; i++ {
		result[i] = entries[i].Query
	}
	return result
}
