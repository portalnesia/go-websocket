package redis

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	coderws "github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
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

	errs := make(chan error, 1)

	cfg := &Config{
		ErrorCallback: func(err error) {
			select {
			case errs <- err:
			default:
			}
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
	select {
	case err := <-errs:
		if err.Error() == "" {
			t.Error("Error message should not be empty")
		}
	case <-time.After(2 * time.Second):
		t.Error("Error callback should be called for invalid JSON")
	}
}

func TestNewUniversalClient(t *testing.T) {
	mr, _ := setupTestRedis(t)
	defer mr.Close()

	uc := redis.NewUniversalClient(&redis.UniversalOptions{Addrs: []string{mr.Addr()}})
	defer uc.Close()

	adapter, err := New(uc)
	if err != nil {
		t.Fatalf("New(UniversalClient) error = %v", err)
	}
	adapter.Close()
}

// node is one websocket.Server with its own Redis adapter, served over httptest
type node struct {
	server *websocket.Server
	ns     *websocket.Namespace
	url    string
}

func newNode(t *testing.T, mr *miniredis.Miniredis, client redis.UniversalClient) *node {
	t.Helper()
	adapter, err := New(client)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	s := websocket.NewServer()
	s.SetLogger(websocket.NewNoopLogger())
	s.SetOriginPatterns([]string{"*"})
	s.SetPingInterval(0)
	s.SetAdapter(adapter)
	ns := s.Of("/")
	ns.OnConnect(func(ctx *websocket.ConnContext) error {
		if room := ctx.Query("room"); room != "" {
			ctx.JoinRoom(room)
		}
		return nil
	})

	srv := httptest.NewServer(s.Handler())
	t.Cleanup(func() {
		srv.Close()
		s.Close()
	})

	channel := adapter.makeNamespaceChannel("/")
	deadline := time.Now().Add(2 * time.Second)
	for mr.PubSubNumSub(channel)[channel] == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	return &node{server: s, ns: ns, url: "ws" + strings.TrimPrefix(srv.URL, "http") + websocket.DefaultWSPath}
}

// dial connects a client to the node and waits until the node registered it.
// Query "room" makes the connection join that room (see newNode).
func (n *node) dial(t *testing.T, query ...string) *coderws.Conn {
	t.Helper()
	before := n.ns.ConnectionCount()
	url := n.url
	if len(query) > 0 {
		url += "?" + query[0]
	}
	c, _, err := coderws.Dial(context.Background(), url, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = c.CloseNow() })
	deadline := time.Now().Add(2 * time.Second)
	for n.ns.ConnectionCount() == before && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	return c
}

// readEvent reads the next message within timeout; ok=false when nothing arrives
func readEvent(c *coderws.Conn, timeout time.Duration) (websocket.Message, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	var msg websocket.Message
	if err := wsjson.Read(ctx, c, &msg); err != nil {
		return msg, false
	}
	return msg, true
}

func TestTwoServersForwardBroadcast(t *testing.T) {
	mr, client := setupTestRedis(t)
	defer mr.Close()
	defer client.Close()
	uc := redis.NewUniversalClient(&redis.UniversalOptions{Addrs: []string{mr.Addr()}})
	defer uc.Close()

	a := newNode(t, mr, client)
	b := newNode(t, mr, uc)
	ca := a.dial(t)
	cb := b.dial(t)

	a.ns.Emit(websocket.Message{Event: "from-a"})
	if msg, ok := readEvent(cb, 2*time.Second); !ok || msg.Event != "from-a" {
		t.Errorf("node B client got %+v ok=%v, want from-a", msg, ok)
	}

	b.ns.Emit(websocket.Message{Event: "from-b"})
	// ca also received from-a locally first
	for i := 0; i < 2; i++ {
		msg, ok := readEvent(ca, 2*time.Second)
		if !ok {
			t.Fatal("node A client did not receive from-b")
		}
		if msg.Event == "from-b" {
			return
		}
	}
	t.Error("node A client did not receive from-b")
}

func TestEmitToRoomsAcrossNodes(t *testing.T) {
	mr, client := setupTestRedis(t)
	defer mr.Close()
	defer client.Close()

	a := newNode(t, mr, client)
	b := newNode(t, mr, client)
	inRoom := b.dial(t, "room=r2")
	outside := b.dial(t, "room=r9")

	a.ns.EmitToRooms([]string{"r1", "r2"}, websocket.Message{Event: "rooms"})
	if msg, ok := readEvent(inRoom, 2*time.Second); !ok || msg.Event != "rooms" {
		t.Errorf("client in r2 on node B got %+v ok=%v, want rooms", msg, ok)
	}
	if msg, ok := readEvent(outside, 200*time.Millisecond); ok {
		t.Errorf("client outside target rooms received %+v", msg)
	}

	a.ns.EmitToRoom("r2", websocket.Message{Event: "room"})
	if msg, ok := readEvent(inRoom, 2*time.Second); !ok || msg.Event != "room" {
		t.Errorf("EmitToRoom: client in r2 got %+v ok=%v, want room", msg, ok)
	}
}

func TestCloseRoomsAcrossNodes(t *testing.T) {
	mr, client := setupTestRedis(t)
	defer mr.Close()
	defer client.Close()

	a := newNode(t, mr, client)
	b := newNode(t, mr, client)
	inRoom := b.dial(t, "room=r1")
	outside := b.dial(t, "room=r9")

	a.ns.CloseRooms([]string{"r1"}, 4401, "unauthorized")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, _, err := inRoom.Read(ctx)
	var ce coderws.CloseError
	if !errors.As(err, &ce) || ce.Code != 4401 || ce.Reason != "unauthorized" {
		t.Errorf("client in r1 on node B: close = %v, want 4401 unauthorized", err)
	}

	b.ns.Emit(websocket.Message{Event: "still-open"})
	if msg, ok := readEvent(outside, 2*time.Second); !ok || msg.Event != "still-open" {
		t.Errorf("client outside r1 should stay open, got %+v ok=%v", msg, ok)
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
