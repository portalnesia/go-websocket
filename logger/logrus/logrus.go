package logrus

import (
	"github.com/sirupsen/logrus"
	websocket "go.portalnesia.com/websocket"
)

// Adapter wraps logrus.Logger to implement websocket.Logger interface
type Adapter struct {
	logger *logrus.Logger
}

// New creates a new logrus adapter
func New(logger *logrus.Logger) websocket.Logger {
	return &Adapter{logger: logger}
}

func (a *Adapter) Debug(msg string, keysAndValues ...interface{}) {
	a.logger.WithFields(toLogrusFields(keysAndValues...)).Debug(msg)
}

func (a *Adapter) Info(msg string, keysAndValues ...interface{}) {
	a.logger.WithFields(toLogrusFields(keysAndValues...)).Info(msg)
}

func (a *Adapter) Warn(msg string, keysAndValues ...interface{}) {
	a.logger.WithFields(toLogrusFields(keysAndValues...)).Warn(msg)
}

func (a *Adapter) Error(msg string, keysAndValues ...interface{}) {
	a.logger.WithFields(toLogrusFields(keysAndValues...)).Error(msg)
}

func toLogrusFields(keysAndValues ...interface{}) logrus.Fields {
	fields := make(logrus.Fields)
	for i := 0; i < len(keysAndValues); i += 2 {
		if i+1 < len(keysAndValues) {
			key := toString(keysAndValues[i])
			fields[key] = keysAndValues[i+1]
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
