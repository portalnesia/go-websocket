package zerolog

import (
	"github.com/rs/zerolog"
	websocket "go.portalnesia.com/websocket"
)

// Adapter wraps zerolog.Logger to implement websocket.Logger interface
type Adapter struct {
	logger zerolog.Logger
}

// New creates a new zerolog adapter
func New(logger zerolog.Logger) websocket.Logger {
	return &Adapter{logger: logger}
}

func (a *Adapter) Debug(msg string, keysAndValues ...interface{}) {
	event := a.logger.Debug()
	for i := 0; i < len(keysAndValues); i += 2 {
		if i+1 < len(keysAndValues) {
			key := toString(keysAndValues[i])
			event = event.Interface(key, keysAndValues[i+1])
		}
	}
	event.Msg(msg)
}

func (a *Adapter) Info(msg string, keysAndValues ...interface{}) {
	event := a.logger.Info()
	for i := 0; i < len(keysAndValues); i += 2 {
		if i+1 < len(keysAndValues) {
			key := toString(keysAndValues[i])
			event = event.Interface(key, keysAndValues[i+1])
		}
	}
	event.Msg(msg)
}

func (a *Adapter) Warn(msg string, keysAndValues ...interface{}) {
	event := a.logger.Warn()
	for i := 0; i < len(keysAndValues); i += 2 {
		if i+1 < len(keysAndValues) {
			key := toString(keysAndValues[i])
			event = event.Interface(key, keysAndValues[i+1])
		}
	}
	event.Msg(msg)
}

func (a *Adapter) Error(msg string, keysAndValues ...interface{}) {
	event := a.logger.Error()
	for i := 0; i < len(keysAndValues); i += 2 {
		if i+1 < len(keysAndValues) {
			key := toString(keysAndValues[i])
			event = event.Interface(key, keysAndValues[i+1])
		}
	}
	event.Msg(msg)
}

func toString(v interface{}) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}
