package log

import (
	"context"

	"github.com/google/uuid"
)

type contextKey string

const sessionIDKey contextKey = "session_id"

func WithSessionID(ctx context.Context, sessionID string) context.Context {
	if sessionID == "" {
		sessionID = uuid.New().String()
	}
	return context.WithValue(ctx, sessionIDKey, sessionID)
}

func GetSessionID(ctx context.Context) string {
	if ctx == nil {
		return uuid.New().String()
	}
	if sessionID, ok := ctx.Value(sessionIDKey).(string); ok && sessionID != "" {
		return sessionID
	}
	return uuid.New().String()
}
