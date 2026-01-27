package websocket

import (
	"bytes"
	"log"
	"strings"
	"testing"
)

func TestDefaultLoggerDebug(t *testing.T) {
	var buf bytes.Buffer
	logger := &defaultLogger{
		logger: log.New(&buf, "[websocket] ", 0),
	}

	logger.Debug("test message", "key1", "value1", "key2", 123)

	output := buf.String()
	if !strings.Contains(output, "[DEBUG]") {
		t.Error("Debug log should contain [DEBUG]")
	}
	if !strings.Contains(output, "test message") {
		t.Error("Debug log should contain message")
	}
	if !strings.Contains(output, "key1=value1") {
		t.Error("Debug log should contain key1=value1")
	}
	if !strings.Contains(output, "key2=123") {
		t.Error("Debug log should contain key2=123")
	}
}

func TestDefaultLoggerInfo(t *testing.T) {
	var buf bytes.Buffer
	logger := &defaultLogger{
		logger: log.New(&buf, "[websocket] ", 0),
	}

	logger.Info("info message", "status", "ok")

	output := buf.String()
	if !strings.Contains(output, "[INFO]") {
		t.Error("Info log should contain [INFO]")
	}
	if !strings.Contains(output, "info message") {
		t.Error("Info log should contain message")
	}
	if !strings.Contains(output, "status=ok") {
		t.Error("Info log should contain status=ok")
	}
}

func TestDefaultLoggerWarn(t *testing.T) {
	var buf bytes.Buffer
	logger := &defaultLogger{
		logger: log.New(&buf, "[websocket] ", 0),
	}

	logger.Warn("warning message", "code", 404)

	output := buf.String()
	if !strings.Contains(output, "[WARN]") {
		t.Error("Warn log should contain [WARN]")
	}
	if !strings.Contains(output, "warning message") {
		t.Error("Warn log should contain message")
	}
	if !strings.Contains(output, "code=404") {
		t.Error("Warn log should contain code=404")
	}
}

func TestDefaultLoggerError(t *testing.T) {
	var buf bytes.Buffer
	logger := &defaultLogger{
		logger: log.New(&buf, "[websocket] ", 0),
	}

	logger.Error("error message", "error", "something went wrong")

	output := buf.String()
	if !strings.Contains(output, "[ERROR]") {
		t.Error("Error log should contain [ERROR]")
	}
	if !strings.Contains(output, "error message") {
		t.Error("Error log should contain message")
	}
	if !strings.Contains(output, "error=something went wrong") {
		t.Error("Error log should contain error message")
	}
}

func TestDefaultLoggerNoKeysAndValues(t *testing.T) {
	var buf bytes.Buffer
	logger := &defaultLogger{
		logger: log.New(&buf, "[websocket] ", 0),
	}

	logger.Info("simple message")

	output := buf.String()
	if !strings.Contains(output, "[INFO]") {
		t.Error("Info log should contain [INFO]")
	}
	if !strings.Contains(output, "simple message") {
		t.Error("Info log should contain message")
	}
}

func TestDefaultLoggerOddKeysAndValues(t *testing.T) {
	var buf bytes.Buffer
	logger := &defaultLogger{
		logger: log.New(&buf, "[websocket] ", 0),
	}

	logger.Debug("odd message", "key1", "value1", "key2")

	output := buf.String()
	if !strings.Contains(output, "key1=value1") {
		t.Error("Should contain key1=value1")
	}
	if !strings.Contains(output, "key2") {
		t.Error("Should contain key2 without value")
	}
}

func TestNewDefaultLogger(t *testing.T) {
	logger := NewDefaultLogger()

	if logger == nil {
		t.Fatal("NewDefaultLogger() should not return nil")
	}

	// Test that it implements Logger interface
	var _ Logger = logger

	// Test that it can be used without panic
	logger.Debug("test")
	logger.Info("test")
	logger.Warn("test")
	logger.Error("test")
}

func TestNoopLoggerMethods(t *testing.T) {
	logger := &noopLogger{}

	// Test that all methods can be called without panic
	logger.Debug("test", "key", "value")
	logger.Info("test", "key", "value")
	logger.Warn("test", "key", "value")
	logger.Error("test", "key", "value")
}

func TestNewNoopLogger(t *testing.T) {
	logger := NewNoopLogger()

	if logger == nil {
		t.Fatal("NewNoopLogger() should not return nil")
	}

	// Test that it implements Logger interface
	var _ Logger = logger

	// Test that it can be used without panic
	logger.Debug("test")
	logger.Info("test")
	logger.Warn("test")
	logger.Error("test")
}

func TestFormatKeysAndValuesEmpty(t *testing.T) {
	result := formatKeysAndValues()
	if result != "" {
		t.Errorf("formatKeysAndValues() with no args should return empty string, got %q", result)
	}
}

func TestFormatKeysAndValuesSinglePair(t *testing.T) {
	result := formatKeysAndValues("key", "value")
	if !strings.Contains(result, "key=value") {
		t.Errorf("formatKeysAndValues() should contain 'key=value', got %q", result)
	}
}

func TestFormatKeysAndValuesMultiplePairs(t *testing.T) {
	result := formatKeysAndValues("key1", "value1", "key2", 123)
	if !strings.Contains(result, "key1=value1") {
		t.Error("Should contain key1=value1")
	}
	if !strings.Contains(result, "key2=123") {
		t.Error("Should contain key2=123")
	}
}

func TestFormatKeysAndValuesOddNumber(t *testing.T) {
	result := formatKeysAndValues("key1", "value1", "key2")
	if !strings.Contains(result, "key1=value1") {
		t.Error("Should contain key1=value1")
	}
	if !strings.Contains(result, "key2") {
		t.Error("Should contain key2")
	}
}

func TestFormatKeysAndValuesVariousTypes(t *testing.T) {
	result := formatKeysAndValues("string", "value", "int", 42, "bool", true, "float", 3.14)

	expected := []string{"string=value", "int=42", "bool=true", "float=3.14"}
	for _, exp := range expected {
		if !strings.Contains(result, exp) {
			t.Errorf("formatKeysAndValues() should contain %q, got %q", exp, result)
		}
	}
}

func TestLoggerInterfaceImplementations(t *testing.T) {
	// Test that both loggers implement the Logger interface
	var _ Logger = &defaultLogger{}
	var _ Logger = &noopLogger{}
	var _ Logger = NewDefaultLogger()
	var _ Logger = NewNoopLogger()
}

func BenchmarkDefaultLoggerDebug(b *testing.B) {
	var buf bytes.Buffer
	logger := &defaultLogger{
		logger: log.New(&buf, "[websocket] ", 0),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		logger.Debug("test message", "key1", "value1", "key2", 123)
	}
}

func BenchmarkDefaultLoggerInfo(b *testing.B) {
	var buf bytes.Buffer
	logger := &defaultLogger{
		logger: log.New(&buf, "[websocket] ", 0),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		logger.Info("test message")
	}
}

func BenchmarkNoopLoggerDebug(b *testing.B) {
	logger := NewNoopLogger()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		logger.Debug("test message", "key1", "value1", "key2", 123)
	}
}

func BenchmarkNoopLoggerInfo(b *testing.B) {
	logger := NewNoopLogger()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		logger.Info("test message")
	}
}

func BenchmarkFormatKeysAndValues(b *testing.B) {
	keysAndValues := []interface{}{"key1", "value1", "key2", 123, "key3", true}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = formatKeysAndValues(keysAndValues...)
	}
}
