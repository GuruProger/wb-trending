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

	log.Println("Сценарий 1: Бот пытается накрутить 'iphone 17'")
	log.Println("Анти-бот должен пропустить только первый запрос, остальные отфильтровать")
	for i := 0; i < 5; i++ {
		sendEvent(writer, "iphone 17", "bot-user-456")
		time.Sleep(200 * time.Millisecond)
	}

	log.Println("\nСценарий 2: Легитимный пользователь ищет разные товар")
	log.Println("Все запросы должны пройти, так как запросы разные")
	legitQueries := []string{"кроссовки nike", "ноутбук asus", "наушники airpods", "кроссовки adidas"}
	for _, query := range legitQueries {
		sendEvent(writer, query, "legit-user-789")
		time.Sleep(200 * time.Millisecond)
	}

	log.Println("\nСценарий 3: Бот продолжает спамить")
	log.Println("Все еще должен отфильтровываться")
	for i := 0; i < 3; i++ {
		sendEvent(writer, "iphone 17", "bot-user-456")
		time.Sleep(200 * time.Millisecond)
	}

	log.Println("\nСценарий 4: Ждем 11 секунд (восстановление токена)")
	time.Sleep(11 * time.Second)

	log.Println("\nСценарий 5: Бот снова пытается (токен восстановился)")
	log.Println("Первый запрос должен пройти")
	for i := 0; i < 2; i++ {
		sendEvent(writer, "iphone 17", "bot-user-456")
		time.Sleep(200 * time.Millisecond)
	}

	log.Println("\nСценарий 6: Другой бот с тем же запросом")
	log.Println("Должен пройти, так как это другой user_id")
	sendEvent(writer, "iphone 17", "another-bot-999")

	log.Println("\nГотово! Проверь виджет: http://localhost:8080/api/v1/trending?limit=10")
}

func sendEvent(writer *kafka.Writer, query, userID string) {
	event := models.SearchEvent{
		Query:     query,
		Timestamp: time.Now().UnixMilli(),
		UserID:    userID,
	}

	data, _ := json.Marshal(event)
	err := writer.WriteMessages(context.Background(), kafka.Message{
		Value: data,
	})

	if err != nil {
		log.Printf("Ошибка отправки: %v", err)
	} else {
		log.Printf("Отправлено: query='%s', user='%s'", event.Query, event.UserID)
	}
}
