package storage

import (
	"fmt"
	"testing"
	"time"
)

// BenchmarkAntiBot_Allow_FirstRequest тестирует первый запрос от нового пользователя.
func BenchmarkAntiBot_Allow_FirstRequest(b *testing.B) {
	ab := NewAntiBot(100000, 1, 10*time.Second)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		userID := fmt.Sprintf("user-%d", i)
		query := fmt.Sprintf("query-%d", i)
		ab.Allow(userID, query)
	}
}

// BenchmarkAntiBot_Allow_Cached тестирует повторные запросы (попадание в кэш).
func BenchmarkAntiBot_Allow_Cached(b *testing.B) {
	ab := NewAntiBot(100000, 1, 10*time.Second)
	userID := "user-123"
	query := "iphone 17"

	// Первый запрос создаёт запись
	ab.Allow(userID, query)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ab.Allow(userID, query)
	}
}

// BenchmarkAntiBot_Allow_Parallel тестирует конкурентный доступ к анти-боту.
// Показывает, как система держит нагрузку от множества пользователей одновременно.
func BenchmarkAntiBot_Allow_Parallel(b *testing.B) {
	ab := NewAntiBot(100000, 1, 10*time.Second)

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			userID := fmt.Sprintf("user-%d", i%1000)
			query := fmt.Sprintf("query-%d", i%100)
			ab.Allow(userID, query)
			i++
		}
	})
}

// BenchmarkAntiBot_Allow_Blocked тестирует путь, когда запрос заблокирован.
// При DDoS-атаке 90% запросов будут идти по этому пути.
func BenchmarkAntiBot_Allow_Blocked(b *testing.B) {
	ab := NewAntiBot(100000, 1, 10*time.Second)
	userID := "user-123"
	query := "iphone 17"

	// Используем единственный токен
	ab.Allow(userID, query)

	b.ResetTimer()
	// Все последующие запросы будут заблокированы
	for i := 0; i < b.N; i++ {
		ab.Allow(userID, query)
	}
}
