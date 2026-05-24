package models

type SearchEvent struct {
	Query string `json:"query"`

	Timestamp int64 `json:"timestamp"`

	UserID string `json:"user_id"`
}
