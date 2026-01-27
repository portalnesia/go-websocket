package websocket

import (
	"fmt"
	"log"
	"os"
)

// Logger interface for custom logging implementations
// Compatible with most logging libraries (zerolog, zap, logrus, etc.)
type Logger interface {
	Debug(msg string, keysAndValues ...interface{})
	Info(msg string, keysAndValues ...interface{})
	Warn(msg string, keysAndValues ...interface{})
	Error(msg string, keysAndValues ...interface{})
}

// defaultLogger implements Logger using standard library log package
type defaultLogger struct {
	logger *log.Logger
}

// NewDefaultLogger creates a new default logger using standard library
func NewDefaultLogger() Logger {
	return &defaultLogger{
		logger: log.New(os.Stdout, "[websocket] ", log.LstdFlags),
	}
}

func (l *defaultLogger) Debug(msg string, keysAndValues ...interface{}) {
	l.logger.Printf("[DEBUG] %s %s", msg, formatKeysAndValues(keysAndValues...))
}

func (l *defaultLogger) Info(msg string, keysAndValues ...interface{}) {
	l.logger.Printf("[INFO] %s %s", msg, formatKeysAndValues(keysAndValues...))
}

func (l *defaultLogger) Warn(msg string, keysAndValues ...interface{}) {
	l.logger.Printf("[WARN] %s %s", msg, formatKeysAndValues(keysAndValues...))
}

func (l *defaultLogger) Error(msg string, keysAndValues ...interface{}) {
	l.logger.Printf("[ERROR] %s %s", msg, formatKeysAndValues(keysAndValues...))
}

// formatKeysAndValues formats key-value pairs for logging
func formatKeysAndValues(keysAndValues ...interface{}) string {
	if len(keysAndValues) == 0 {
		return ""
	}

	result := ""
	for i := 0; i < len(keysAndValues); i += 2 {
		if i+1 < len(keysAndValues) {
			result += fmt.Sprintf("%v=%v ", keysAndValues[i], keysAndValues[i+1])
		} else {
			result += fmt.Sprintf("%v ", keysAndValues[i])
		}
	}
	return result
}

// NoopLogger is a logger that does nothing - for disabling logs
type noopLogger struct{}

func (n *noopLogger) Debug(msg string, keysAndValues ...interface{}) {}
func (n *noopLogger) Info(msg string, keysAndValues ...interface{})  {}
func (n *noopLogger) Warn(msg string, keysAndValues ...interface{})  {}
func (n *noopLogger) Error(msg string, keysAndValues ...interface{}) {}

// NewNoopLogger creates a logger that does nothing
func NewNoopLogger() Logger {
	return &noopLogger{}
}
