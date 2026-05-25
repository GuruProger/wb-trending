package storage

import (
	"sync"
	"testing"
	"time"

	"github.com/GuruProger/wb-trending/internal/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRingBufferStorage_BasicAddAndGetTop(t *testing.T) {
	storage := NewRingBufferStorage(5*time.Minute, time.Second)

	// Добавляем события
	storage.Add(models.SearchEvent{Query: "iphone", Timestamp: time.Now().UnixMilli(), UserID: "user1"})
	storage.Add(models.SearchEvent{Query: "iphone", Timestamp: time.Now().UnixMilli(), UserID: "user2"})
	storage.Add(models.SearchEvent{Query: "samsung", Timestamp: time.Now().UnixMilli(), UserID: "user1"})

	// Проверяем топ
	top := storage.GetTop(10)
	require.Len(t, top, 2)
	assert.Equal(t, "iphone", top[0], "iphone должен быть первым (2 запроса)")
	assert.Equal(t, "samsung", top[1], "samsung должен быть вторым (1 запрос)")
}

func TestRingBufferStorage_Limit(t *testing.T) {
	storage := NewRingBufferStorage(5*time.Minute, time.Second)

	// Добавляем 5 разных запросов
	for i := 0; i < 5; i++ {
		storage.Add(models.SearchEvent{
			Query:     "query" + string(rune('0'+i)),
			Timestamp: time.Now().UnixMilli(),
			UserID:    "user1",
		})
	}

	// Запрашиваем топ-3
	top := storage.GetTop(3)
	assert.Len(t, top, 3, "Должно вернуться ровно 3 элемента")
}

func TestRingBufferStorage_StopListIntegration(t *testing.T) {
	storage := NewRingBufferStorage(5*time.Minute, time.Second)

	// Блокируем слово
	storage.GetStopList().Add("blocked")

	// Добавляем заблокированное и разрешенное слова
	storage.Add(models.SearchEvent{Query: "blocked", Timestamp: time.Now().UnixMilli(), UserID: "user1"})
	storage.Add(models.SearchEvent{Query: "allowed", Timestamp: time.Now().UnixMilli(), UserID: "user1"})

	// Проверяем топ
	top := storage.GetTop(10)
	require.Len(t, top, 1)
	assert.Equal(t, "allowed", top[0], "Заблокированное слово не должно попасть в топ")
}

func TestRingBufferStorage_AntiBotIntegration(t *testing.T) {
	storage := NewRingBufferStorage(5*time.Minute, time.Second)

	// Первый запрос проходит
	storage.Add(models.SearchEvent{Query: "iphone", Timestamp: time.Now().UnixMilli(), UserID: "user1"})

	// Повторные запросы от того же пользователя блокируются анти-ботом
	storage.Add(models.SearchEvent{Query: "iphone", Timestamp: time.Now().UnixMilli(), UserID: "user1"})
	storage.Add(models.SearchEvent{Query: "iphone", Timestamp: time.Now().UnixMilli(), UserID: "user1"})

	// Но от другого пользователя проходит
	storage.Add(models.SearchEvent{Query: "iphone", Timestamp: time.Now().UnixMilli(), UserID: "user2"})

	top := storage.GetTop(10)
	require.Len(t, top, 1)
	assert.Equal(t, "iphone", top[0])

	// Проверяем счетчик (должен быть 2, а не 4)
	// Для этого нужно проверить внутреннее состояние
	storage.mu.RLock()
	count := storage.aggregated["iphone"]
	storage.mu.RUnlock()

	assert.Equal(t, 2, count, "Анти-бот должен отфильтровать дубликаты")
}

func TestRingBufferStorage_EmptyResult(t *testing.T) {
	storage := NewRingBufferStorage(5*time.Minute, time.Second)

	// Пустое хранилище
	top := storage.GetTop(10)
	assert.Empty(t, top, "Пустое хранилище должно возвращать пустой результат")
}

func TestRingBufferStorage_OutOfOrderEvents(t *testing.T) {
	storage := NewRingBufferStorage(5*time.Minute, time.Second)

	now := time.Now().UnixMilli()

	// Добавляем событие из "будущего"
	storage.Add(models.SearchEvent{Query: "future", Timestamp: now + 10000, UserID: "user1"})

	// Добавляем событие из "прошлого" (в пределах окна)
	storage.Add(models.SearchEvent{Query: "past", Timestamp: now - 5000, UserID: "user1"})

	// Добавляем текущее событие
	storage.Add(models.SearchEvent{Query: "present", Timestamp: now, UserID: "user1"})

	top := storage.GetTop(10)
	assert.Len(t, top, 3, "Все события в пределах окна должны быть учтены")
}

func TestRingBufferStorage_OldEventsIgnored(t *testing.T) {
	storage := NewRingBufferStorage(5*time.Minute, time.Second)

	// Событие старше 5 минут
	oldTimestamp := time.Now().Add(-6 * time.Minute).UnixMilli()
	storage.Add(models.SearchEvent{Query: "old", Timestamp: oldTimestamp, UserID: "user1"})

	// Текущее событие
	storage.Add(models.SearchEvent{Query: "new", Timestamp: time.Now().UnixMilli(), UserID: "user1"})

	top := storage.GetTop(10)
	require.Len(t, top, 1)
	assert.Equal(t, "new", top[0], "Старые события должны игнорироваться")
}

func TestRingBufferStorage_CacheTTL(t *testing.T) {
	storage := NewRingBufferStorage(5*time.Minute, time.Second)
	storage.cacheTTL = 100 * time.Millisecond // Уменьшаем TTL для теста

	storage.Add(models.SearchEvent{Query: "iphone", Timestamp: time.Now().UnixMilli(), UserID: "user1"})

	// Первый вызов
	top1 := storage.GetTop(10)
	require.Len(t, top1, 1)

	// Сразу второй вызов - должен вернуться кэш
	top2 := storage.GetTop(10)
	assert.Equal(t, top1, top2, "Повторный вызов должен вернуть кэшированный результат")

	// Ждем истечения TTL
	time.Sleep(150 * time.Millisecond)

	// Добавляем новое событие
	storage.Add(models.SearchEvent{Query: "samsung", Timestamp: time.Now().UnixMilli(), UserID: "user1"})

	// Третий вызов - должен пересчитать
	top3 := storage.GetTop(10)
	assert.Len(t, top3, 2, "После истечения TTL должен пересчитать")
}

func TestRingBufferStorage_BucketRotation(t *testing.T) {
	// Маленькое окно для быстрого теста (10 секунд)
	storage := NewRingBufferStorage(10*time.Second, time.Second)

	now := time.Now().Unix()

	// Добавляем событие в текущую секунду
	storage.Add(models.SearchEvent{Query: "query1", Timestamp: now * 1000, UserID: "user1"})

	// Добавляем событие через 11 секунд (старый бакет должен очиститься)
	futureTime := (now + 11) * 1000
	storage.Add(models.SearchEvent{Query: "query2", Timestamp: futureTime, UserID: "user1"})

	top := storage.GetTop(10)
	require.Len(t, top, 1)
	assert.Equal(t, "query2", top[0], "Старое событие должно быть вытеснено")
}

func TestRingBufferStorage_SameQueryMultipleUsers(t *testing.T) {
	storage := NewRingBufferStorage(5*time.Minute, time.Second)

	// Один запрос от 5 разных пользователей
	for i := 0; i < 5; i++ {
		storage.Add(models.SearchEvent{
			Query:     "popular",
			Timestamp: time.Now().UnixMilli(),
			UserID:    "user" + string(rune('0'+i)),
		})
	}

	top := storage.GetTop(10)
	require.Len(t, top, 1)

	storage.mu.RLock()
	count := storage.aggregated["popular"]
	storage.mu.RUnlock()

	assert.Equal(t, 5, count, "Один запрос от разных пользователей должен считаться 5 раз")
}

func TestRingBufferStorage_MultipleQueriesSorting(t *testing.T) {
	storage := NewRingBufferStorage(5*time.Minute, time.Second)

	// Добавляем запросы с разной частотой
	for i := 0; i < 10; i++ {
		storage.Add(models.SearchEvent{Query: "popular", Timestamp: time.Now().UnixMilli(), UserID: "user" + string(rune('0'+i))})
	}
	for i := 0; i < 5; i++ {
		storage.Add(models.SearchEvent{Query: "medium", Timestamp: time.Now().UnixMilli(), UserID: "user" + string(rune('0'+i))})
	}
	for i := 0; i < 1; i++ {
		storage.Add(models.SearchEvent{Query: "rare", Timestamp: time.Now().UnixMilli(), UserID: "user1"})
	}

	top := storage.GetTop(10)
	require.Len(t, top, 3)
	assert.Equal(t, "popular", top[0], "Самый популярный должен быть первым")
	assert.Equal(t, "medium", top[1], "Средний по популярности - вторым")
	assert.Equal(t, "rare", top[2], "Редкий - третьим")
}

func TestRingBufferStorage_GetTop_ConcurrentAccess(t *testing.T) {
	storage := NewRingBufferStorage(5*time.Minute, time.Second)

	// Заполняем данными
	for i := 0; i < 100; i++ {
		storage.Add(models.SearchEvent{
			Query:     "query" + string(rune('0'+i%10)),
			Timestamp: time.Now().UnixMilli(),
			UserID:    "user" + string(rune('0'+i)),
		})
	}

	var wg sync.WaitGroup

	// Запускаем 50 горутин на чтение
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = storage.GetTop(10)
		}()
	}

	// И 50 горутин на запись
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			storage.Add(models.SearchEvent{
				Query:     "query",
				Timestamp: time.Now().UnixMilli(),
				UserID:    "user",
			})
		}(i)
	}

	wg.Wait()
	// Если дошли сюда без паники и deadlock - тест пройден
}

func TestRingBufferStorage_IncrementalAggregation(t *testing.T) {
	storage := NewRingBufferStorage(5*time.Minute, time.Second)

	// Добавляем одно и то же событие 100 раз
	for i := 0; i < 100; i++ {
		storage.Add(models.SearchEvent{
			Query:     "test",
			Timestamp: time.Now().UnixMilli(),
			UserID:    "user" + string(rune('0'+i%10)), // 10 разных пользователей
		})
	}

	// Проверяем, что агрегатор правильно посчитал
	storage.mu.RLock()
	count := storage.aggregated["test"]
	storage.mu.RUnlock()

	// Должно быть 10 (по одному от каждого пользователя, анти-бот не дает больше)
	assert.Equal(t, 10, count, "Инкрементальная агрегация должна работать корректно")
}
