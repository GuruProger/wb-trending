package storage

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/GuruProger/wb-trending/internal/models"
)

// BenchmarkAdd_Single тестирует последовательное добавление событий.
// Показывает базовую производительность без конкуренции.
func BenchmarkAdd_Single(b *testing.B) {
	storage := NewRingBufferStorage(5*time.Minute, time.Second)
	event := models.SearchEvent{
		Query:     "iphone 17",
		Timestamp: time.Now().UnixMilli(),
		UserID:    "user-123",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		storage.Add(event)
	}
}

// BenchmarkAdd_Parallel тестирует параллельное добавление событий.
func BenchmarkAdd_Parallel(b *testing.B) {
	storage := NewRingBufferStorage(5*time.Minute, time.Second)

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			event := models.SearchEvent{
				Query:     fmt.Sprintf("query-%d", i%1000), // 1000 уникальных запросов
				Timestamp: time.Now().UnixMilli(),
				UserID:    fmt.Sprintf("user-%d", i%100), // 100 уникальных пользователей
			}
			storage.Add(event)
			i++
		}
	})
}

// BenchmarkGetTop_Cached тестирует чтение из кэша (быстрый путь).
// Большинство запросов к API должны попадать в этот сценарий благодаря cacheTTL.
func BenchmarkGetTop_Cached(b *testing.B) {
	storage := NewRingBufferStorage(5*time.Minute, time.Second)

	// Заполняем хранилище данными
	for i := 0; i < 1000; i++ {
		storage.Add(models.SearchEvent{
			Query:     fmt.Sprintf("query-%d", i),
			Timestamp: time.Now().UnixMilli(),
			UserID:    "user-123",
		})
	}

	// Первый вызов заполняет кэш
	storage.GetTop(10)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		storage.GetTop(10)
	}
}

// BenchmarkGetTop_Cached_Parallel тестирует параллельное чтение из кэша.
// Показывает, как система держит concurrent read requests.
func BenchmarkGetTop_Cached_Parallel(b *testing.B) {
	storage := NewRingBufferStorage(5*time.Minute, time.Second)

	// Заполняем хранилище данными
	for i := 0; i < 1000; i++ {
		storage.Add(models.SearchEvent{
			Query:     fmt.Sprintf("query-%d", i),
			Timestamp: time.Now().UnixMilli(),
			UserID:    "user-123",
		})
	}

	// Первый вызов заполняет кэш
	storage.GetTop(10)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			storage.GetTop(10)
		}
	})
}

// BenchmarkGetTop_Rebuild тестирует чтение с пересборкой кэша (медленный путь).
func BenchmarkGetTop_Rebuild(b *testing.B) {
	storage := NewRingBufferStorage(5*time.Minute, time.Second)
	storage.cacheTTL = 0 // Отключаем кэш для теста

	// Заполняем хранилище данными
	for i := 0; i < 1000; i++ {
		storage.Add(models.SearchEvent{
			Query:     fmt.Sprintf("query-%d", i),
			Timestamp: time.Now().UnixMilli(),
			UserID:    "user-123",
		})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		storage.GetTop(10)
	}
}

// BenchmarkMixed_ReadWrite тестирует смешанную нагрузку (80% чтение, 20% запись).
func BenchmarkMixed_ReadWrite(b *testing.B) {
	storage := NewRingBufferStorage(5*time.Minute, time.Second)

	// Заполняем начальными данными
	for i := 0; i < 100; i++ {
		storage.Add(models.SearchEvent{
			Query:     fmt.Sprintf("query-%d", i),
			Timestamp: time.Now().UnixMilli(),
			UserID:    "user-123",
		})
	}

	// Первый вызов заполняет кэш
	storage.GetTop(10)

	var wg sync.WaitGroup
	b.ResetTimer()

	// 80% горутин читают
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < b.N; j++ {
				storage.GetTop(10)
			}
		}()
	}

	// 20% горутин пишут
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < b.N; j++ {
				storage.Add(models.SearchEvent{
					Query:     fmt.Sprintf("query-%d", j%1000),
					Timestamp: time.Now().UnixMilli(),
					UserID:    fmt.Sprintf("user-%d", id),
				})
			}
		}(i)
	}

	wg.Wait()
}
