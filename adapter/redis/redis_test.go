package redis

import (
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	websocket "go.portalnesia.com/websocket"
)

func setupTestRedis(t *testing.T) (*miniredis.Miniredis, *redis.Client) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("Failed to start miniredis: %v", err)
	}

	client := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})

	return mr, client
}

func TestNew(t *testing.T) {
	mr, client := setupTestRedis(t)
	defer mr.Close()
	defer client.Close()

	t.Run("Create adapter with default config", func(t *testing.T) {
		adapter, err := New(client)
		if err != nil {
			t.Fatalf("New() error = %v", err)
		}
		defer adapter.Close()

		if adapter == nil {
			t.Fatal("New() should not return nil")
		}
	})

	t.Run("Create adapter with custom config", func(t *testing.T) {
		errorCalled := false
		cfg := &Config{
			MaxPublishGoros: 50,
			ErrorCallback: func(err error) {
				errorCalled = true
			},
		}

		adapter, err := New(client, cfg)
		if err != nil {
			t.Fatalf("New() error = %v", err)
		}
		defer adapter.Close()

		if adapter == nil {
			t.Fatal("New() should not return nil")
		}

		// Verify config was applied
		if cap(adapter.publishSem) != 50 {
			t.Errorf("publishSem capacity = %d, want 50", cap(adapter.publishSem))
		}

		if adapter.errorCallback == nil {
			t.Error("errorCallback should be set")
		}

		_ = errorCalled // May be used in error scenarios
	})

	t.Run("Connection failure", func(t *testing.T) {
		badClient := redis.NewClient(&redis.Options{
			Addr: "localhost:99999", // Invalid port
		})
		defer badClient.Close()

		_, err := New(badClient)
		if err == nil {
			t.Error("New() with bad client should return error")
		}
	})
}

func TestAdapterSetLogger(t *testing.T) {
	mr, client := setupTestRedis(t)
	defer mr.Close()
	defer client.Close()

	adapter, _ := New(client)
	defer adapter.Close()

	t.Run("Set custom logger", func(t *testing.T) {
		logger := websocket.NewNoopLogger()
		adapter.SetLogger(logger)

		if adapter.logger == nil {
			t.Error("logger should be set")
		}
	})

	t.Run("Set nil logger", func(t *testing.T) {
		adapter.SetLogger(nil)
		logger := adapter.GetLogger()

		if logger == nil {
			t.Error("GetLogger() should never return nil")
		}
	})
}

func TestAdapterGetLogger(t *testing.T) {
	mr, client := setupTestRedis(t)
	defer mr.Close()
	defer client.Close()

	adapter, _ := New(client)
	defer adapter.Close()

	logger := adapter.GetLogger()
	if logger == nil {
		t.Fatal("GetLogger() should never return nil")
	}

	// Test with nil logger
	adapter.logger = nil
	logger = adapter.GetLogger()
	if logger == nil {
		t.Error("GetLogger() should return NoopLogger when logger is nil")
	}
}

func TestAdapterBroadcast(t *testing.T) {
	mr, client := setupTestRedis(t)
	defer mr.Close()
	defer client.Close()

	adapter, _ := New(client)
	adapter.SetLogger(websocket.NewNoopLogger())
	defer adapter.Close()

	t.Run("Broadcast message", func(t *testing.T) {
		data := websocket.BroadcastData{
			Message: websocket.MessageMultiNodes{
				Message: websocket.Message{
					Event: "test",
					Data:  "hello",
				},
			},
		}

		adapter.Broadcast("/test", data)

		// Give some time for async publish
		time.Sleep(100 * time.Millisecond)

		// Message should be published (we can't easily check with miniredis CheckList for pub/sub)
		// Just verify no panic occurred
	})

	t.Run("Broadcast with room", func(t *testing.T) {
		data := websocket.BroadcastData{
			Room: "room1",
			Message: websocket.MessageMultiNodes{
				Message: websocket.Message{
					Event: "room-event",
					Data:  "room data",
				},
			},
		}

		adapter.Broadcast("/test", data)

		time.Sleep(100 * time.Millisecond)
	})
}

func TestAdapterSubscribe(t *testing.T) {
	mr, client := setupTestRedis(t)
	defer mr.Close()
	defer client.Close()

	adapter, _ := New(client)
	adapter.SetLogger(websocket.NewNoopLogger())
	defer adapter.Close()

	t.Run("Subscribe to namespace", func(t *testing.T) {
		received := make(chan websocket.BroadcastData, 1)
		handler := func(data websocket.BroadcastData) {
			received <- data
		}

		adapter.Subscribe("/test", handler)

		// Give time for subscription to be ready
		time.Sleep(100 * time.Millisecond)

		// Publish message
		data := websocket.BroadcastData{
			Message: websocket.MessageMultiNodes{
				Message: websocket.Message{
					Event: "test-event",
					Data:  "test-data",
				},
			},
		}

		channel := adapter.makeNamespaceChannel("/test")
		jsonData, _ := json.Marshal(data)
		mr.Publish(channel, string(jsonData))

		// Wait for message
		select {
		case receivedData := <-received:
			if receivedData.Message.Message.Event != "test-event" {
				t.Errorf("Received event = %s, want test-event", receivedData.Message.Message.Event)
			}
		case <-time.After(2 * time.Second):
			t.Error("Did not receive message within timeout")
		}
	})

	t.Run("Subscribe multiple times - update handler", func(t *testing.T) {
		handler1 := func(data websocket.BroadcastData) {}
		handler2 := func(data websocket.BroadcastData) {}

		adapter.Subscribe("/multi", handler1)
		adapter.Subscribe("/multi", handler2)

		// Should only have one subscription
		adapter.subMu.RLock()
		count := len(adapter.subscriptions)
		adapter.subMu.RUnlock()

		if count == 0 {
			t.Error("Should have subscription for /multi")
		}
	})
}

func TestAdapterUnsubscribe(t *testing.T) {
	mr, client := setupTestRedis(t)
	defer mr.Close()
	defer client.Close()

	adapter, _ := New(client)
	adapter.SetLogger(websocket.NewNoopLogger())
	defer adapter.Close()

	t.Run("Unsubscribe existing namespace", func(t *testing.T) {
		handler := func(data websocket.BroadcastData) {}
		adapter.Subscribe("/test", handler)

		time.Sleep(50 * time.Millisecond)

		adapter.Unsubscribe("/test")

		// Check subscription was removed
		adapter.subMu.RLock()
		_, exists := adapter.subscriptions["/test"]
		adapter.subMu.RUnlock()

		if exists {
			t.Error("Subscription should be removed")
		}
	})

	t.Run("Unsubscribe non-existing namespace", func(t *testing.T) {
		// Should not panic
		adapter.Unsubscribe("/non-existing")
	})
}

func TestAdapterClose(t *testing.T) {
	mr, client := setupTestRedis(t)
	defer mr.Close()
	defer client.Close()

	t.Run("Close adapter", func(t *testing.T) {
		adapter, _ := New(client)
		adapter.SetLogger(websocket.NewNoopLogger())

		// Add some subscriptions
		adapter.Subscribe("/ns1", func(data websocket.BroadcastData) {})
		adapter.Subscribe("/ns2", func(data websocket.BroadcastData) {})

		time.Sleep(50 * time.Millisecond)

		adapter.Close()

		// Check subscriptions are cleared
		adapter.subMu.RLock()
		subs := adapter.subscriptions
		adapter.subMu.RUnlock()

		if subs != nil {
			t.Error("subscriptions should be nil after Close()")
		}
	})

	t.Run("Close with pending publishes", func(t *testing.T) {
		adapter, _ := New(client)
		adapter.SetLogger(websocket.NewNoopLogger())

		// Start many publishes
		for i := 0; i < 10; i++ {
			adapter.Broadcast("/test", websocket.BroadcastData{
				Message: websocket.MessageMultiNodes{
					Message: websocket.Message{Event: "test", Data: i},
				},
			})
		}

		// Close should wait for them
		adapter.Close()
	})
}

func TestAdapterMakeNamespaceChannel(t *testing.T) {
	mr, client := setupTestRedis(t)
	defer mr.Close()
	defer client.Close()

	adapter, _ := New(client)
	defer adapter.Close()

	tests := []struct {
		namespace string
		expected  string
	}{
		{"/", "pnws:ns:/"},
		{"/chat", "pnws:ns:/chat"},
		{"/test", "pnws:ns:/test"},
	}

	for _, tt := range tests {
		t.Run(tt.namespace, func(t *testing.T) {
			channel := adapter.makeNamespaceChannel(tt.namespace)
			if channel != tt.expected {
				t.Errorf("makeNamespaceChannel(%s) = %s, want %s", tt.namespace, channel, tt.expected)
			}
		})
	}
}

func TestAdapterConcurrentPublish(t *testing.T) {
	mr, client := setupTestRedis(t)
	defer mr.Close()
	defer client.Close()

	adapter, _ := New(client, &Config{
		MaxPublishGoros: 10,
	})
	adapter.SetLogger(websocket.NewNoopLogger())
	defer adapter.Close()

	const goroutines = 50
	const msgsPerGoroutine = 20

	wg := sync.WaitGroup{}
	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < msgsPerGoroutine; j++ {
				adapter.Broadcast("/test", websocket.BroadcastData{
					Message: websocket.MessageMultiNodes{
						Message: websocket.Message{
							Event: "test",
							Data:  map[string]int{"id": id, "msg": j},
						},
					},
				})
			}
		}(i)
	}

	wg.Wait()

	// Wait for all publishes to complete
	time.Sleep(200 * time.Millisecond)
}

func TestAdapterSubscribeAndBroadcast(t *testing.T) {
	mr, client := setupTestRedis(t)
	defer mr.Close()
	defer client.Close()

	adapter, _ := New(client)
	adapter.SetLogger(websocket.NewNoopLogger())
	defer adapter.Close()

	received := make(chan websocket.BroadcastData, 10)
	handler := func(data websocket.BroadcastData) {
		received <- data
	}

	adapter.Subscribe("/chat", handler)

	time.Sleep(100 * time.Millisecond)

	// Broadcast multiple messages
	messages := []string{"msg1", "msg2", "msg3"}
	for _, msg := range messages {
		adapter.Broadcast("/chat", websocket.BroadcastData{
			Message: websocket.MessageMultiNodes{
				Message: websocket.Message{
					Event: "chat",
					Data:  msg,
				},
			},
		})
	}

	// Collect received messages
	receivedMsgs := make([]string, 0)
	timeout := time.After(2 * time.Second)

	for i := 0; i < len(messages); i++ {
		select {
		case data := <-received:
			if dataStr, ok := data.Message.Message.Data.(string); ok {
				receivedMsgs = append(receivedMsgs, dataStr)
			}
		case <-timeout:
			t.Fatalf("Timeout waiting for messages, got %d of %d", len(receivedMsgs), len(messages))
		}
	}

	if len(receivedMsgs) != len(messages) {
		t.Errorf("Received %d messages, want %d", len(receivedMsgs), len(messages))
	}
}

func TestAdapterErrorCallback(t *testing.T) {
	mr, client := setupTestRedis(t)
	defer mr.Close()
	defer client.Close()

	errorCalled := false
	var errorMsg string

	cfg := &Config{
		ErrorCallback: func(err error) {
			errorCalled = true
			errorMsg = err.Error()
		},
	}

	adapter, _ := New(client, cfg)
	adapter.SetLogger(websocket.NewNoopLogger())
	defer adapter.Close()

	// Subscribe with handler that will receive invalid JSON
	adapter.Subscribe("/test", func(data websocket.BroadcastData) {})

	time.Sleep(100 * time.Millisecond)

	// Publish invalid JSON
	channel := adapter.makeNamespaceChannel("/test")
	mr.Publish(channel, "invalid json{{{")

	// Wait for error callback
	time.Sleep(200 * time.Millisecond)

	if !errorCalled {
		t.Error("Error callback should be called for invalid JSON")
	}

	if errorMsg == "" {
		t.Error("Error message should not be empty")
	}
}

func BenchmarkAdapterBroadcast(b *testing.B) {
	mr, _ := miniredis.Run()
	defer mr.Close()

	client := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})
	defer client.Close()

	adapter, _ := New(client)
	adapter.SetLogger(websocket.NewNoopLogger())
	defer adapter.Close()

	data := websocket.BroadcastData{
		Message: websocket.MessageMultiNodes{
			Message: websocket.Message{
				Event: "test",
				Data:  "benchmark",
			},
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		adapter.Broadcast("/test", data)
	}
}

func BenchmarkAdapterMakeNamespaceChannel(b *testing.B) {
	mr, _ := miniredis.Run()
	defer mr.Close()

	client := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})
	defer client.Close()

	adapter, _ := New(client)
	defer adapter.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = adapter.makeNamespaceChannel("/test")
	}
}
