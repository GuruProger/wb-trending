package storage

import "github.com/GuruProger/wb-trending/internal/models"

// TrendingStorage определяет контракт для in-memory хранилища.
// Интерфейс позволяет легко заменить наивную реализацию на оптимизированную (Ring Buffer)
// без изменения бизнес-логики в API и Consumer.
type TrendingStorage interface {
	Add(event models.SearchEvent)
	GetTop(limit int) []string
}
