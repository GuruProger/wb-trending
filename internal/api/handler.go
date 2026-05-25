package api

import (
	"encoding/json"
	"log"
	"net/http"
	"strconv"

	"github.com/GuruProger/wb-trending/internal/storage"
	"github.com/go-chi/chi/v5"
)

type Handler struct {
	storage storage.TrendingStorage
}

func NewHandler(store storage.TrendingStorage) *Handler {
	return &Handler{storage: store}
}

func (h *Handler) RegisterRoutes(r chi.Router) {
	r.Get("/api/v1/trending", h.getTrending)
	r.Get("/health", h.healthCheck)
}

func (h *Handler) getTrending(w http.ResponseWriter, r *http.Request) {
	limitStr := r.URL.Query().Get("limit")
	if limitStr == "" {
		limitStr = "10"
	}

	limit, err := strconv.Atoi(limitStr)
	if err != nil || limit < 1 {
		http.Error(w, "Invalid limit parameter", http.StatusBadRequest)
		return
	}

	if limit > 100 {
		limit = 100
	}

	top := h.storage.GetTop(limit)

	w.Header().Set("Content-Type", "application/json")

	response := map[string]interface{}{
		"queries": top,
		"count":   len(top),
	}

	if err := json.NewEncoder(w).Encode(response); err != nil {
		// Headers уже отправлены, поэтому просто логируем ошибку
		log.Printf("Ошибка кодирования JSON ответа: %v", err)
	}
}

func (h *Handler) healthCheck(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write([]byte("OK")); err != nil {
		log.Printf("Ошибка записи health check ответа: %v", err)
	}
}
