package api

import (
	"encoding/json"
	"log"
	"net/http"
	"strings"

	"github.com/GuruProger/wb-trending/internal/storage"
)

func (h *Handler) getStopList(w http.ResponseWriter, r *http.Request) {
	store, ok := h.storage.(*storage.RingBufferStorage)
	if !ok {
		log.Printf("Ошибка приведения типа хранилища к RingBufferStorage")
		http.Error(w, "Хранилище не поддерживает стоп-лист", http.StatusInternalServerError)
		return
	}

	words := store.GetStopList().GetAll()

	w.Header().Set("Content-Type", "application/json")

	if err := json.NewEncoder(w).Encode(map[string]interface{}{
		"words": words,
		"count": len(words),
	}); err != nil {
		log.Printf("Ошибка кодирования JSON ответа: %v", err)
	}
}

func (h *Handler) addToStopList(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Word string `json:"word"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		log.Printf("Ошибка декодирования JSON запроса: %v", err)
		http.Error(w, "Некорректный JSON", http.StatusBadRequest)
		return
	}

	word := strings.TrimSpace(req.Word)
	if word == "" {
		http.Error(w, "Слово не может быть пустым", http.StatusBadRequest)
		return
	}

	store, ok := h.storage.(*storage.RingBufferStorage)
	if !ok {
		log.Printf("Ошибка приведения типа хранилища к RingBufferStorage")
		http.Error(w, "Хранилище не поддерживает стоп-лист", http.StatusInternalServerError)
		return
	}

	store.GetStopList().Add(word)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)

	if err := json.NewEncoder(w).Encode(map[string]interface{}{
		"message": "Слово добавлено в стоп-лист",
		"word":    word,
	}); err != nil {
		log.Printf("Ошибка кодирования JSON ответа: %v", err)
	}
}

func (h *Handler) removeFromStopList(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Word string `json:"word"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		log.Printf("Ошибка декодирования JSON запроса: %v", err)
		http.Error(w, "Некорректный JSON", http.StatusBadRequest)
		return
	}

	word := strings.TrimSpace(req.Word)
	if word == "" {
		http.Error(w, "Слово не может быть пустым", http.StatusBadRequest)
		return
	}

	store, ok := h.storage.(*storage.RingBufferStorage)
	if !ok {
		log.Printf("Ошибка приведения типа хранилища к RingBufferStorage")
		http.Error(w, "Хранилище не поддерживает стоп-лист", http.StatusInternalServerError)
		return
	}

	store.GetStopList().Remove(word)

	w.Header().Set("Content-Type", "application/json")

	if err := json.NewEncoder(w).Encode(map[string]interface{}{
		"message": "Слово удалено из стоп-листа",
		"word":    word,
	}); err != nil {
		log.Printf("Ошибка кодирования JSON ответа: %v", err)
	}
}
