package config

import (
	"os"
	"time"
)

type Config struct {
	Server  ServerConfig
	Kafka   KafkaConfig
	Storage StorageConfig
}

type ServerConfig struct {
	Port string
}

type KafkaConfig struct {
	Brokers     []string
	Topic       string
	GroupID     string
	StartOffset int64
}

type StorageConfig struct {
	WindowSize time.Duration // Размер скользящего окна (5 минут)
	BucketSize time.Duration // Дискретизация окна для оптимизации памяти
}

// Load читает конфигурацию из env.
func Load() *Config {
	return &Config{
		Server: ServerConfig{
			Port: getEnv("SERVER_PORT", "8080"),
		},
		Kafka: KafkaConfig{
			Brokers:     []string{getEnv("KAFKA_BROKERS", "localhost:9092")},
			Topic:       getEnv("KAFKA_TOPIC", "search_events"),
			GroupID:     getEnv("KAFKA_GROUP_ID", "trending_service"),
			StartOffset: -1, // Эквивалент kafka.FirstOffset (читать с самого начала при старте)
		},
		Storage: StorageConfig{
			WindowSize: 5 * time.Minute,
			// Разбиваем 5 минут на 300 бакетов по 1 секунде.
			BucketSize: 1 * time.Second,
		},
	}
}

func getEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}
