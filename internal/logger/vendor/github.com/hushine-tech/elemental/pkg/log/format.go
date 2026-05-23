package log

import (
	"encoding/json"
	"os"
	"unicode/utf8"

	"github.com/google/uuid"
)

const (
	MaxMessageLen = 65536
)

type Level string

const (
	LevelDebug Level = "DEBUG"
	LevelInfo  Level = "INFO"
	LevelWarn  Level = "WARN"
	LevelError Level = "ERROR"
	LevelFatal Level = "FATAL"
)

func (l Level) String() string {
	return string(l)
}

func getHost() string {
	if host := os.Getenv("HOSTNAME"); host != "" {
		return host
	}

	hostname, err := os.Hostname()
	if err == nil && hostname != "" {
		return hostname
	}

	return generateFallbackHost()
}

func generateFallbackHost() string {
	id := uuid.New().String()
	return id[:8]
}

func truncateMessage(msg string) string {
	if utf8.RuneCountInString(msg) <= MaxMessageLen {
		return msg
	}

	truncated := msg
	for len(truncated) > MaxMessageLen-15 {
		_, size := utf8.DecodeLastRuneInString(truncated)
		truncated = truncated[:len(truncated)-size]
	}
	return truncated + "...(truncated)"
}

func marshalJSON(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}
