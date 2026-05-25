package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/GuruProger/wb-trending/internal/models"
	"github.com/segmentio/kafka-go"
)

func main() {
	writer := &kafka.Writer{
		Addr:     kafka.TCP("localhost:9092"),
		Topic:    "search_events",
		Balancer: &kafka.LeastBytes{},
	}
	defer func() {
		if err := writer.Close(); err != nil {
			log.Printf("Ошибка закрытия writer: %v", err)
		}
	}()

	queries := []string{
		"iphone 17",
		"кроссовки nike",
		"ноутбук asus",
		"наушники airpods",
		"кроссовки adidas",
		"iphone 17",
		"iphone 17",
		"кроссовки nike",
	}

	for i := 0; i < 50; i++ {
		event := models.SearchEvent{
			Query:     queries[i%len(queries)],
			Timestamp: time.Now().UnixMilli(),
			UserID:    "user-123",
		}

		data, _ := json.Marshal(event)
		err := writer.WriteMessages(context.Background(), kafka.Message{
			Value: data,
		})

		if err != nil {
			log.Printf("Ошибка отправки: %v", err)
		} else {
			log.Printf("Отправлено: %s", event.Query)
		}

		time.Sleep(200 * time.Millisecond)
	}

	log.Println("Готово!")
}
