package storage

import (
	"sync"
	"sync/atomic"
	"time"

	lru "github.com/hashicorp/golang-lru/v2/expirable"
)

// AntiBot защищает от накруток поисковых запросов.
// Использует Token Bucket алгоритм для ограничения частоты одинаковых запросов от одного пользователя.
type AntiBot struct {
	// LRU-кэш с автоматическим удалением старых записей.
	// Жесткий лимит размера защищает от OOM при DDoS-атаках.
	cache *lru.LRU[string, *tokenBucket]

	// Параметры rate limiting
	maxTokens      int           // Максимальное количество токенов
	refillInterval time.Duration // Время восстановления одного токена

	// Счетчики для мониторинга и отладки.
	// Используем atomic, чтобы не блокировать горячий путь мьютексами.
	totalRequests   atomic.Int64
	blockedRequests atomic.Int64
}

type tokenBucket struct {
	mu         sync.Mutex
	tokens     int       // Текущее количество доступных токенов
	lastRefill time.Time // Время последнего пополнения
}

// NewAntiBot создает экземпляр анти-бота с заданными параметрами.
// cacheSize - максимальное количество отслеживаемых пар (userID, query).
// При превышении старые записи автоматически вытесняются.
func NewAntiBot(cacheSize int, maxTokens int, refillInterval time.Duration) *AntiBot {
	// TTL 5 минут соответствует размеру sliding window.
	// Если пользователь не делал запросов 5 минут, его данные удаляются.
	cache := lru.NewLRU[string, *tokenBucket](cacheSize, nil, 5*time.Minute)

	return &AntiBot{
		cache:          cache,
		maxTokens:      maxTokens,
		refillInterval: refillInterval,
	}
}

// Allow проверяет, разрешен ли запрос от пользователя.
// Возвращает true, если запрос следует принять, false - если это накрутка.
func (ab *AntiBot) Allow(userID, query string) bool {
	ab.totalRequests.Add(1)

	key := userID + ":" + query

	bucket, ok := ab.cache.Get(key)
	if !ok {
		// Первый запрос от этого пользователя - создаем полную корзину токенов
		bucket = &tokenBucket{
			tokens:     ab.maxTokens,
			lastRefill: time.Now(),
		}
		ab.cache.Add(key, bucket)
	}

	bucket.mu.Lock()
	defer bucket.mu.Unlock()

	// Пополняем токены в зависимости от прошедшего времени
	now := time.Now()
	elapsed := now.Sub(bucket.lastRefill)
	tokensToAdd := int(elapsed / ab.refillInterval)

	if tokensToAdd > 0 {
		bucket.tokens += tokensToAdd
		if bucket.tokens > ab.maxTokens {
			bucket.tokens = ab.maxTokens
		}
		bucket.lastRefill = now
	}

	// Проверяем наличие токенов
	if bucket.tokens > 0 {
		bucket.tokens--
		return true
	}

	ab.blockedRequests.Add(1)
	return false
}

// Stats возвращает накопленную статистику работы анти-бота.
// Используется для метрик и отладки, не блокирует основной поток.
func (ab *AntiBot) Stats() (total, blocked int64) {
	return ab.totalRequests.Load(), ab.blockedRequests.Load()
}
