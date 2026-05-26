package storage

import (
	"sync"

	"github.com/GuruProger/wb-trending/internal/metrics"
)

// StopList хранит список заблокированных поисковых запросов.
// Используется для фильтрации нежелательных слов из топа.
type StopList struct {
	mu    sync.RWMutex
	words map[string]bool
}

func NewStopList() *StopList {
	return &StopList{
		words: make(map[string]bool),
	}
}

// Add добавляет слово в стоп-лист
func (s *StopList) Add(word string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.words[word] = true

	// Обновляем метрику после изменения размера
	metrics.StopListSize.Set(float64(len(s.words)))
}

// Remove удаляет слово из стоп-листа
func (s *StopList) Remove(word string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.words, word)

	// Обновляем метрику после изменения размера
	metrics.StopListSize.Set(float64(len(s.words)))
}

// IsBlocked проверяет, находится ли слово в стоп-листе
func (s *StopList) IsBlocked(word string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.words[word]
}

// GetAll возвращает все слова из стоп-листа
func (s *StopList) GetAll() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]string, 0, len(s.words))
	for word := range s.words {
		result = append(result, word)
	}
	return result
}
