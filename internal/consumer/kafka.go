package consumer

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/GuruProger/wb-trending/internal/models"
	"github.com/GuruProger/wb-trending/internal/storage"
	"github.com/segmentio/kafka-go"
)

type KafkaConsumer struct {
	reader  *kafka.Reader
	storage storage.TrendingStorage
}

func NewKafkaConsumer(brokers []string, topic, groupID string, store storage.TrendingStorage) *KafkaConsumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		GroupID:  groupID,
		Topic:    topic,
		MinBytes: 1e3,             // Минимальный размер батча (1KB)
		MaxBytes: 10e6,            // Максимальный размер батча (10MB)
		MaxWait:  1 * time.Second, // Максимальное время ожидания батча
	})

	return &KafkaConsumer{
		reader:  reader,
		storage: store,
	}
}

// Start запускает чтение сообщений в бесконечном цикле.
// Блокирует выполнение до вызова Stop или отмены контекста.
func (c *KafkaConsumer) Start(ctx context.Context) error {
	log.Println("Kafka consumer запущен")

	for {
		msg, err := c.reader.ReadMessage(ctx)
		if err != nil {
			// Контекст отменен - завершаем работу
			if ctx.Err() != nil {
				log.Println("Kafka consumer остановлен")
				return nil
			}
			log.Printf("Ошибка чтения из Kafka: %v", err)
			continue
		}

		var event models.SearchEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			log.Printf("Ошибка десериализации сообщения: %v", err)
			continue
		}

		log.Printf("Получено событие: query=%s, user=%s", event.Query, event.UserID)

		c.storage.Add(event)
	}
}

// Stop закрывает соединение с Kafka.
func (c *KafkaConsumer) Stop() error {
	return c.reader.Close()
}
