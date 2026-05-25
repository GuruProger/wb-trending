package api

import (
	"github.com/GuruProger/wb-trending/internal/storage"
	"github.com/go-chi/chi/v5"
	"log"
	"net/http"
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

	r.Route("/api/v1/stoplist", func(r chi.Router) {
		r.Get("/", h.getStopList)
		r.Post("/", h.addToStopList)
		r.Delete("/", h.removeFromStopList)
	})
}

func (h *Handler) healthCheck(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write([]byte("OK")); err != nil {
		log.Printf("Ошибка записи health check ответа: %v", err)
	}
}
