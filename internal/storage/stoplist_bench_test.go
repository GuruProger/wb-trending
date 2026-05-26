package storage

import (
	"testing"
)

// BenchmarkStopList_IsBlocked тестирует проверку блокировки (самая частая операция).
func BenchmarkStopList_IsBlocked(b *testing.B) {
	sl := NewStopList()
	sl.Add("blocked_word")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sl.IsBlocked("blocked_word")
	}
}

// BenchmarkStopList_IsBlocked_NotFound тестирует проверку несуществующего слова.
func BenchmarkStopList_IsBlocked_NotFound(b *testing.B) {
	sl := NewStopList()
	sl.Add("blocked_word")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sl.IsBlocked("allowed_word")
	}
}

// BenchmarkStopList_IsBlocked_Parallel тестирует конкурентное чтение.
func BenchmarkStopList_IsBlocked_Parallel(b *testing.B) {
	sl := NewStopList()
	sl.Add("blocked_word")
	sl.Add("another_blocked")

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if i%2 == 0 {
				sl.IsBlocked("blocked_word")
			} else {
				sl.IsBlocked("allowed_word")
			}
			i++
		}
	})
}
