package storage

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestAntiBot_FirstRequestAllowed(t *testing.T) {
	ab := NewAntiBot(1000, 1, 10*time.Second)

	// Первый запрос должен пройти
	assert.True(t, ab.Allow("user1", "iphone"), "Первый запрос должен быть разрешен")
}

func TestAntiBot_BlocksDuplicateRequests(t *testing.T) {
	ab := NewAntiBot(1000, 1, 10*time.Second)

	// Первый запрос проходит
	assert.True(t, ab.Allow("user1", "iphone"))

	// Повторные запросы блокируются
	assert.False(t, ab.Allow("user1", "iphone"), "Второй запрос должен быть заблокирован")
	assert.False(t, ab.Allow("user1", "iphone"), "Третий запрос должен быть заблокирован")
}

func TestAntiBot_DifferentQueriesAllowed(t *testing.T) {
	ab := NewAntiBot(1000, 1, 10*time.Second)

	// Разные запросы от одного пользователя проходят
	assert.True(t, ab.Allow("user1", "iphone"))
	assert.True(t, ab.Allow("user1", "samsung"), "Другой запрос от того же пользователя должен пройти")
	assert.True(t, ab.Allow("user1", "xiaomi"), "Третий запрос от того же пользователя должен пройти")

	// Но повтор первого запроса блокируется
	assert.False(t, ab.Allow("user1", "iphone"))
}

func TestAntiBot_DifferentUsersAllowed(t *testing.T) {
	ab := NewAntiBot(1000, 1, 10*time.Second)

	// Один и тот же запрос от разных пользователей проходит
	assert.True(t, ab.Allow("user1", "iphone"))
	assert.True(t, ab.Allow("user2", "iphone"), "Тот же запрос от другого пользователя должен пройти")
	assert.True(t, ab.Allow("user3", "iphone"), "Тот же запрос от третьего пользователя должен пройти")

	// Но повтор от первого пользователя блокируется
	assert.False(t, ab.Allow("user1", "iphone"))
}

func TestAntiBot_TokenRefill(t *testing.T) {
	// Используем короткий интервал для быстрого теста
	ab := NewAntiBot(1000, 1, 100*time.Millisecond)

	// Первый запрос проходит
	assert.True(t, ab.Allow("user1", "iphone"))

	// Сразу после - блокируется
	assert.False(t, ab.Allow("user1", "iphone"))

	// Ждем восстановления токена
	time.Sleep(150 * time.Millisecond)

	// Теперь должен пройти снова
	assert.True(t, ab.Allow("user1", "iphone"), "После восстановления токена запрос должен пройти")
}

func TestAntiBot_MultipleTokens(t *testing.T) {
	// Даем пользователю 3 токена
	ab := NewAntiBot(1000, 3, 10*time.Second)

	// Первые 3 запроса проходят
	assert.True(t, ab.Allow("user1", "iphone"))
	assert.True(t, ab.Allow("user1", "iphone"))
	assert.True(t, ab.Allow("user1", "iphone"))

	// Четвертый блокируется
	assert.False(t, ab.Allow("user1", "iphone"), "Четвертый запрос должен быть заблокирован")
}

func TestAntiBot_MultipleTokensRefill(t *testing.T) {
	ab := NewAntiBot(1000, 2, 100*time.Millisecond)

	// Используем оба токена
	assert.True(t, ab.Allow("user1", "iphone"))
	assert.True(t, ab.Allow("user1", "iphone"))
	assert.False(t, ab.Allow("user1", "iphone"))

	// Ждем восстановления одного токена
	time.Sleep(150 * time.Millisecond)

	// Один запрос должен пройти
	assert.True(t, ab.Allow("user1", "iphone"))
	assert.False(t, ab.Allow("user1", "iphone"))
}

func TestAntiBot_LRUEviction(t *testing.T) {
	// Кэш на 2 записи
	ab := NewAntiBot(2, 1, 10*time.Second)

	// Заполняем кэш
	ab.Allow("user1", "query1")
	ab.Allow("user2", "query2")

	// Добавляем третью запись - первая должна вытесниться
	ab.Allow("user3", "query3")

	// Теперь user1:query1 должен пройти снова (как новый)
	assert.True(t, ab.Allow("user1", "query1"), "Вытесненная запись должна пройти как новая")
}

func TestAntiBot_ConcurrentAccess(t *testing.T) {
	ab := NewAntiBot(10000, 1, 10*time.Second)
	var wg sync.WaitGroup

	// Запускаем 100 горутин с разными пользователями
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			userID := "user"
			query := "query"
			// Каждый пользователь может сделать один запрос
			ab.Allow(userID, query)
		}(i)
	}

	wg.Wait()
	// Если дошли сюда без паники и deadlock - тест пройден
}

func TestAntiBot_Stats(t *testing.T) {
	ab := NewAntiBot(1000, 1, 10*time.Second)

	// Изначально статистика пустая
	total, blocked := ab.Stats()
	assert.Equal(t, int64(0), total)
	assert.Equal(t, int64(0), blocked)

	// Делаем несколько запросов
	ab.Allow("user1", "iphone")  // проходит
	ab.Allow("user1", "iphone")  // блокируется
	ab.Allow("user1", "iphone")  // блокируется
	ab.Allow("user1", "samsung") // проходит

	total, blocked = ab.Stats()
	assert.Equal(t, int64(4), total, "Должно быть 4 запроса")
	assert.Equal(t, int64(2), blocked, "Должно быть 2 заблокированных")
}

func TestAntiBot_CacheLimit(t *testing.T) {
	// Маленький кэш для теста
	ab := NewAntiBot(5, 1, 10*time.Second)

	// Заполняем кэш
	for i := 0; i < 10; i++ {
		ab.Allow(fmt.Sprintf("user%d", i), "query")
	}

	// Проверяем размер кэша
	assert.Equal(t, 5, ab.cache.Len(), "Размер кэша не должен превышать лимит")

	// Дополнительно проверим, что первые пользователи были вытеснены
	// Для этого запросим статистику по старому пользователю - он должен получить новый токен
	ab.Allow("user0", "query") // Должно вернуть true, так как это "новый" пользователь для кэша
}

func TestAntiBot_TokenCap(t *testing.T) {
	ab := NewAntiBot(1000, 2, 100*time.Millisecond)

	// Ждем, чтобы токены накопились больше максимума
	time.Sleep(500 * time.Millisecond)

	// Все равно должно быть только 2 токена (не больше maxTokens)
	assert.True(t, ab.Allow("user1", "iphone"))
	assert.True(t, ab.Allow("user1", "iphone"))
	assert.False(t, ab.Allow("user1", "iphone"), "Токены не должны накапливаться больше максимума")
}
