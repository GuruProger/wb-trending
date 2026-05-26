package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/GuruProger/wb-trending/internal/metrics"
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
			// Считаем битые сообщения отдельно, чтобы замечать проблемы со смежным сервисом
			metrics.EventsProcessedTotal.WithLabelValues("invalid_json").Inc()
			continue
		}

		// Валидируем и нормализуем событие перед обработкой.
		// Делаем это до замера лага, чтобы не искажать метрику невалидными данными.
		if !c.validateAndNormalize(&event) {
			continue
		}

		// Измеряем лаг - разницу между timestamp события и временем его обработки.
		// Если эта метрика растёт, значит consumer не успевает за потоком.
		lag := time.Since(time.UnixMilli(event.Timestamp)).Seconds()
		if lag >= 0 {
			metrics.KafkaMessageLag.Observe(lag)
		}

		// Замеряем время выполнения storage.Add() - самой горячей операции на пути записи.
		// p99 этой метрики покажет, деградирует ли in-memory структура с ростом данных.
		start := time.Now()
		c.storage.Add(event)
		metrics.ConsumerProcessingDuration.Observe(time.Since(start).Seconds())
	}
}

// validateAndNormalize проверяет обязательные поля события и нормализует их при необходимости.
// Возвращает false, если событие нужно отбросить (например, пустой query).
func (c *KafkaConsumer) validateAndNormalize(event *models.SearchEvent) bool {
	// Пустой запрос не имеет смысла для топа - отбрасываем.
	// Без этого в агрегатор может попасть пустая строка, что сломает сортировку.
	if event.Query == "" {
		metrics.EventsProcessedTotal.WithLabelValues("invalid_query").Inc()
		return false
	}

	// Если timestamp невалидный (например, смежный сервис не проставил его), используем текущее время.
	// Это лучше, чем отбрасывать событие целиком, потому что сам факт поиска важен для топа.
	if event.Timestamp <= 0 {
		event.Timestamp = time.Now().UnixMilli()
	}

	//Присваиваем id для пользователя без id
	if event.UserID == "" {
		event.UserID = fmt.Sprintf("anon-%d", time.Now().UnixNano())
	}

	return true
}

// Stop закрывает соединение с Kafka.
func (c *KafkaConsumer) Stop() error {
	return c.reader.Close()
}
