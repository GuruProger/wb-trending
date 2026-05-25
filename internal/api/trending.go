package api

import (
	"encoding/json"
	"log"
	"net/http"
	"strconv"
)

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
		log.Printf("Ошибка кодирования JSON ответа: %v", err)
	}
}
