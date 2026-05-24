package storage

import (
	"sort"
	"sync"
	"time"

	"github.com/GuruProger/wb-trending/internal/models"
)

type Bucket struct {
	Timestamp int64
	Queries   map[string]int
}

// RingBufferStorage использует кольцевой буфер для хранения данных за последние 5 минут.
// Вместо хранения каждого события отдельно, мы разбиваем время на 1-секундные интервалы.
// Получаем быстрое очищение устаревших данных и эффективное использование памяти.
type RingBufferStorage struct {
	mu sync.RWMutex

	buckets     []*Bucket
	bucketCount int

	// Глобальный счетчик всех запросов за текущее окно.
	// Обновляется при каждом добавлении события, чтобы не пересчитывать топ с нуля.
	aggregated map[string]int

	currentSecond int64

	cachedTop       []cachedEntry
	cacheValidUntil time.Time
	cacheTTL        time.Duration
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
	}
}

func (s *RingBufferStorage) Add(event models.SearchEvent) {
	eventSecond := event.Timestamp / 1000

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.currentSecond == 0 {
		s.currentSecond = eventSecond
	}

	// Событие пришло слишком поздно (старше 5 минут) - игнорируем
	if eventSecond < s.currentSecond-int64(s.bucketCount) {
		return
	}

	// Событие из прошлого (в пределах окна) - добавляем в нужный интервал
	if eventSecond < s.currentSecond {
		s.incrementBucket(eventSecond, event.Query)
		return
	}

	// Событие из будущего - очищаем все промежутки между текущим и новым временем
	for sec := s.currentSecond + 1; sec <= eventSecond; sec++ {
		s.moveToNewBucket(sec)
	}
	s.currentSecond = eventSecond

	s.incrementBucket(eventSecond, event.Query)
}

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

	bucket.Queries[query]++
	s.aggregated[query]++
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
	}
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

func (s *RingBufferStorage) rebuildCache(limit int) []string {
	s.mu.Lock()
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
