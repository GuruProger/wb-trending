package api

import (
	"net/http"
	"strconv"
	"time"

	"github.com/GuruProger/wb-trending/internal/metrics"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

// excludedPaths содержит служебные эндпоинты, которые не должны попадать в бизнес-метрики API.
// /health - liveness probe от балансировщика, вызывается каждые несколько секунд.
// /metrics - сам сборщик Prometheus, не имеет отношения к пользовательской нагрузке.
var excludedPaths = map[string]bool{
	"/health":  true,
	"/metrics": true,
}

// MetricsMiddleware считает количество HTTP-запросов к API.
// Ставится самым внешним в цепочке middleware, чтобы измерять полное время ответа сервера клиенту,
// включая работу всех нижележащих middleware.
func MetricsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Служебные эндпоинты пропускаем без замеров
		if excludedPaths[r.URL.Path] {
			next.ServeHTTP(w, r)
			return
		}

		start := time.Now()
		// WrapResponseWriter из chi позволяет узнать статус-код после записи ответа.
		ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)

		next.ServeHTTP(ww, r)

		// Берем паттерн роута (например "/api/v1/stoplist/"), а не конкретный URL.
		// Иначе запросы с разными query-параметрами создадут миллион уникальных метрик
		// и Prometheus перестанет справляться.
		routePattern := r.URL.Path
		if rctx := chi.RouteContext(r.Context()); rctx != nil {
			if pattern := rctx.RoutePattern(); pattern != "" {
				routePattern = pattern
			}
		}

		duration := time.Since(start).Seconds()
		status := strconv.Itoa(ww.Status())

		metrics.APIRequestDuration.WithLabelValues(r.Method, routePattern, status).Observe(duration)
		metrics.APIRequestsTotal.WithLabelValues(r.Method, routePattern, status).Inc()
	})
}
