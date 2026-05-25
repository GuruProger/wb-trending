package storage

import (
	"time"

	"github.com/GuruProger/wb-trending/internal/models"
)

// TrendingStorage определяет контракт для in-memory хранилища.
type TrendingStorage interface {
	Add(event models.SearchEvent)
	GetTop(limit int) []string
}

// NewStorage создает экземпляр хранилища с заданными параметрами окна.
func NewStorage(windowSize, bucketSize time.Duration) TrendingStorage {
	return NewRingBufferStorage(windowSize, bucketSize)
}
