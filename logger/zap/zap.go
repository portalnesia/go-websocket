package zap

import (
	websocket "go.portalnesia.com/websocket"
	"go.uber.org/zap"
)

// Adapter wraps zap.Logger to implement websocket.Logger interface
type Adapter struct {
	logger *zap.Logger
}

// New creates a new zap adapter
func New(logger *zap.Logger) websocket.Logger {
	return &Adapter{logger: logger}
}

func (a *Adapter) Debug(msg string, keysAndValues ...interface{}) {
	a.logger.Debug(msg, toZapFields(keysAndValues...)...)
}

func (a *Adapter) Info(msg string, keysAndValues ...interface{}) {
	a.logger.Info(msg, toZapFields(keysAndValues...)...)
}

func (a *Adapter) Warn(msg string, keysAndValues ...interface{}) {
	a.logger.Warn(msg, toZapFields(keysAndValues...)...)
}

func (a *Adapter) Error(msg string, keysAndValues ...interface{}) {
	a.logger.Error(msg, toZapFields(keysAndValues...)...)
}

func toZapFields(keysAndValues ...interface{}) []zap.Field {
	fields := make([]zap.Field, 0, len(keysAndValues)/2)
	for i := 0; i < len(keysAndValues); i += 2 {
		if i+1 < len(keysAndValues) {
			key := toString(keysAndValues[i])
			fields = append(fields, zap.Any(key, keysAndValues[i+1]))
		}
	}
	return fields
}

func toString(v interface{}) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}
