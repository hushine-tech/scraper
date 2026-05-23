package log

import (
	"context"
	"sync"
)

var (
	defaultLogger     Logger
	defaultKafkaProd *KafkaProducer
	defaultESProd    *esProducer
	once             sync.Once
	initErr          error
)

func InitLog(configPath string) error {
	once.Do(func() {
		cfg, err := LoadConfig(configPath)
		if err != nil {
			initErr = err
			return
		}

		defaultLogger, err = NewLogger(cfg)
		if err != nil {
			initErr = err
			return
		}

		if cfg.Kafka.Enabled {
			defaultKafkaProd, err = NewKafkaProducer(cfg.Kafka)
			if err != nil {
				defaultLogger.Close()
				initErr = err
				return
			}
		}

		if cfg.Elasticsearch.Enabled {
			defaultESProd = newESProducer(cfg.Elasticsearch)
		}
	})
	return initErr
}

func Close() error {
	var err error
	if defaultKafkaProd != nil {
		if closeErr := defaultKafkaProd.Close(); closeErr != nil {
			err = closeErr
		}
	}
	if defaultESProd != nil {
		if closeErr := defaultESProd.Close(); closeErr != nil {
			err = closeErr
		}
	}
	if defaultLogger != nil {
		if closeErr := defaultLogger.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}
	return err
}

func Info(ctx context.Context, logType string, msg string) {
	sessionID := GetSessionID(ctx)
	entry := buildEntry(logType, msg, LevelInfo, sessionID)

	if defaultLogger != nil {
		defaultLogger.Info(ctx, logType, msg)
	}
	if defaultKafkaProd != nil {
		defaultKafkaProd.Send(ctx, logType, msg, LevelInfo)
	}
	if defaultESProd != nil {
		defaultESProd.send(entry)
	}
}

func Debug(ctx context.Context, logType string, msg string) {
	if defaultLogger != nil {
		defaultLogger.Debug(ctx, logType, msg)
	}
	if defaultKafkaProd != nil {
		defaultKafkaProd.Send(ctx, logType, msg, LevelDebug)
	}
}

func Warn(ctx context.Context, logType string, msg string) {
	if defaultLogger != nil {
		defaultLogger.Warn(ctx, logType, msg)
	}
	if defaultKafkaProd != nil {
		defaultKafkaProd.Send(ctx, logType, msg, LevelWarn)
	}
}

func Error(ctx context.Context, logType string, msg string) {
	if defaultLogger != nil {
		defaultLogger.Error(ctx, logType, msg)
	}
	if defaultKafkaProd != nil {
		defaultKafkaProd.Send(ctx, logType, msg, LevelError)
	}
}

func Fatal(ctx context.Context, logType string, msg string) {
	if defaultLogger != nil {
		defaultLogger.Fatal(ctx, logType, msg)
	}
	if defaultKafkaProd != nil {
		defaultKafkaProd.Send(ctx, logType, msg, LevelFatal)
	}
}

func Access(ctx context.Context, access AccessLogEntry) {
	sessionID := GetSessionID(ctx)
	entry := buildAccessEntry(access, sessionID)

	if defaultLogger != nil {
		defaultLogger.Access(ctx, access)
	}
	if defaultKafkaProd != nil {
		defaultKafkaProd.SendAccess(ctx, access)
	}
	if defaultESProd != nil {
		defaultESProd.send(entry)
	}
}

func ExtAPI(ctx context.Context, extAPI ExtAPILogEntry) {
	sessionID := GetSessionID(ctx)
	entry := buildExtAPIEntry(extAPI, sessionID)

	if defaultLogger != nil {
		defaultLogger.ExtAPI(ctx, extAPI)
	}
	if defaultKafkaProd != nil {
		defaultKafkaProd.SendExtAPI(ctx, extAPI)
	}
	if defaultESProd != nil {
		defaultESProd.send(entry)
	}
}

func SQL(ctx context.Context, sqlLog SQLLogEntry) {
	sessionID := GetSessionID(ctx)
	entry := buildSQLEntry(sqlLog, sessionID)

	if defaultLogger != nil {
		defaultLogger.SQL(ctx, sqlLog)
	}
	if defaultKafkaProd != nil {
		defaultKafkaProd.SendSQL(ctx, sqlLog)
	}
	if defaultESProd != nil {
		defaultESProd.send(entry)
	}
}

func WebSocket(ctx context.Context, ws WebSocketLogEntry) {
	sessionID := GetSessionID(ctx)
	entry := buildWebSocketEntry(ws, sessionID)

	if defaultLogger != nil {
		defaultLogger.WebSocket(ctx, ws)
	}
	if defaultKafkaProd != nil {
		defaultKafkaProd.SendWebSocket(ctx, ws)
	}
	if defaultESProd != nil {
		defaultESProd.send(entry)
	}
}
