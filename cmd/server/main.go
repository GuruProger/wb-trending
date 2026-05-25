package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/GuruProger/wb-trending/internal/api"
	"github.com/GuruProger/wb-trending/internal/config"
	"github.com/GuruProger/wb-trending/internal/consumer"
	"github.com/GuruProger/wb-trending/internal/storage"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

func main() {
	cfg := config.Load()

	// Инициализация хранилища
	store := storage.NewStorage(cfg.Storage.WindowSize, cfg.Storage.BucketSize)

	// Инициализация Kafka consumer
	kafkaConsumer := consumer.NewKafkaConsumer(
		cfg.Kafka.Brokers,
		cfg.Kafka.Topic,
		cfg.Kafka.GroupID,
		store,
	)

	// Настройка HTTP роутера
	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)
	r.Use(middleware.Timeout(10 * time.Second))

	handler := api.NewHandler(store)
	handler.RegisterRoutes(r)

	server := &http.Server{
		Addr:         ":" + cfg.Server.Port,
		Handler:      r,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	// Контекст для graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Запуск Kafka consumer в отдельной горутине
	go func() {
		if err := kafkaConsumer.Start(ctx); err != nil {
			log.Printf("Kafka consumer ошибка: %v", err)
		}
	}()

	// Запуск HTTP сервера
	go func() {
		log.Printf("HTTP сервер запущен на порту %s", cfg.Server.Port)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("Ошибка HTTP сервера: %v", err)
		}
	}()

	// Ожидание сигнала завершения
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("Получен сигнал завершения, начинаем graceful shutdown...")

	// Отмена контекста для остановки consumer'а
	cancel()

	// Остановка Kafka consumer
	if err := kafkaConsumer.Stop(); err != nil {
		log.Printf("Ошибка остановки Kafka consumer: %v", err)
	}

	// Остановка HTTP сервера
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Fatalf("Ошибка при остановке сервера: %v", err)
	}
	log.Println("Сервер успешно остановлен")
}
