package websocket

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

func TestConnContextID(t *testing.T) {
	ctx := &ConnContext{
		id: "test-id-123",
	}

	if ctx.ID() != "test-id-123" {
		t.Errorf("ID() = %s, want test-id-123", ctx.ID())
	}
}

func TestConnContextLocal(t *testing.T) {
	ctx := &ConnContext{
		id: "test-id",
	}

	t.Run("Get non-existing key", func(t *testing.T) {
		val := ctx.Local("key1")
		if val != nil {
			t.Errorf("Local(key1) should return nil for non-existing key, got %v", val)
		}
	})

	t.Run("Set and Get", func(t *testing.T) {
		ctx.Local("key1", "value1")
		val := ctx.Local("key1")
		if val != "value1" {
			t.Errorf("Local(key1) = %v, want value1", val)
		}
	})

	t.Run("Set returns value", func(t *testing.T) {
		ret := ctx.Local("key2", "value2")
		if ret != "value2" {
			t.Errorf("Local set should return value, got %v", ret)
		}
	})

	t.Run("Update existing key", func(t *testing.T) {
		ctx.Local("key1", "updated")
		val := ctx.Local("key1")
		if val != "updated" {
			t.Errorf("Local(key1) = %v, want updated", val)
		}
	})

	t.Run("Delete key with nil", func(t *testing.T) {
		ctx.Local("key1", "value")
		ctx.Local("key1", nil)
		val := ctx.Local("key1")
		if val != nil {
			t.Errorf("After delete, Local(key1) should return nil, got %v", val)
		}
	})

	t.Run("Map cleared after all keys deleted", func(t *testing.T) {
		ctx2 := &ConnContext{id: "test2"} // Use new context to avoid interference
		ctx2.Local("k1", "v1")
		ctx2.Local("k2", "v2")
		ctx2.Local("k1", nil)
		ctx2.Local("k2", nil)

		// After deleting all keys, getting a key should still work (map may or may not be nil)
		val := ctx2.Local("k1")
		if val != nil {
			t.Error("Deleted key should return nil")
		}
	})

	t.Run("Different value types", func(t *testing.T) {
		ctx.Local("string", "test")
		ctx.Local("int", 42)
		ctx.Local("bool", true)
		ctx.Local("struct", map[string]string{"key": "value"})

		if ctx.Local("string") != "test" {
			t.Error("String value mismatch")
		}
		if ctx.Local("int") != 42 {
			t.Error("Int value mismatch")
		}
		if ctx.Local("bool") != true {
			t.Error("Bool value mismatch")
		}
	})
}

func TestConnContextLocalConcurrency(t *testing.T) {
	ctx := &ConnContext{
		id: "test-id",
	}

	const goroutines = 100
	const opsPerGoroutine = 100

	wg := sync.WaitGroup{}
	wg.Add(goroutines * 2)

	// Concurrent writes
	for i := 0; i < goroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				key := generateId()
				ctx.Local(key, id)
			}
		}(i)
	}

	// Concurrent reads
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				_ = ctx.Local("some-key")
			}
		}()
	}

	wg.Wait()
}

func TestConnContextHeader(t *testing.T) {
	req := httptest.NewRequest("GET", "/test", nil)
	req.Header.Set("X-Custom-Header", "test-value")
	req.Header.Set("Authorization", "Bearer token123")

	ctx := &ConnContext{
		id:      "test-id",
		request: req,
	}

	t.Run("Get existing header", func(t *testing.T) {
		val := ctx.Header("X-Custom-Header")
		if val != "test-value" {
			t.Errorf("Header(X-Custom-Header) = %s, want test-value", val)
		}
	})

	t.Run("Get another header", func(t *testing.T) {
		val := ctx.Header("Authorization")
		if val != "Bearer token123" {
			t.Errorf("Header(Authorization) = %s, want Bearer token123", val)
		}
	})

	t.Run("Get non-existing header", func(t *testing.T) {
		val := ctx.Header("Non-Existing")
		if val != "" {
			t.Errorf("Header(Non-Existing) should return empty string, got %s", val)
		}
	})
}

func TestConnContextRequestURL(t *testing.T) {
	req := httptest.NewRequest("GET", "/test?param=value", nil)

	ctx := &ConnContext{
		id:      "test-id",
		request: req,
	}

	reqURL := ctx.RequestURL()
	if reqURL == nil {
		t.Fatal("RequestURL() should not return nil")
	}

	if reqURL.Path != "/test" {
		t.Errorf("RequestURL().Path = %s, want /test", reqURL.Path)
	}
}

func TestConnContextQuery(t *testing.T) {
	req := httptest.NewRequest("GET", "/test?name=john&age=30&empty=", nil)

	ctx := &ConnContext{
		id:      "test-id",
		request: req,
	}

	t.Run("Get existing query param", func(t *testing.T) {
		val := ctx.Query("name")
		if val != "john" {
			t.Errorf("Query(name) = %s, want john", val)
		}
	})

	t.Run("Get another query param", func(t *testing.T) {
		val := ctx.Query("age")
		if val != "30" {
			t.Errorf("Query(age) = %s, want 30", val)
		}
	})

	t.Run("Get empty query param", func(t *testing.T) {
		val := ctx.Query("empty")
		if val != "" {
			t.Errorf("Query(empty) should return empty string, got %s", val)
		}
	})

	t.Run("Get non-existing query param", func(t *testing.T) {
		val := ctx.Query("non-existing")
		if val != "" {
			t.Errorf("Query(non-existing) should return empty string, got %s", val)
		}
	})
}

func TestConnContextCookie(t *testing.T) {
	req := httptest.NewRequest("GET", "/test", nil)
	req.AddCookie(&http.Cookie{
		Name:  "session",
		Value: "abc123",
	})
	req.AddCookie(&http.Cookie{
		Name:  "user",
		Value: "john",
	})

	ctx := &ConnContext{
		id:      "test-id",
		request: req,
	}

	t.Run("Get existing cookie", func(t *testing.T) {
		cookie, err := ctx.Cookie("session")
		if err != nil {
			t.Errorf("Cookie(session) error = %v", err)
		}
		if cookie == nil {
			t.Fatal("Cookie(session) should not return nil")
		}
		if cookie.Value != "abc123" {
			t.Errorf("Cookie(session).Value = %s, want abc123", cookie.Value)
		}
	})

	t.Run("Get another cookie", func(t *testing.T) {
		cookie, err := ctx.Cookie("user")
		if err != nil {
			t.Errorf("Cookie(user) error = %v", err)
		}
		if cookie == nil {
			t.Fatal("Cookie(user) should not return nil")
		}
		if cookie.Value != "john" {
			t.Errorf("Cookie(user).Value = %s, want john", cookie.Value)
		}
	})

	t.Run("Get non-existing cookie", func(t *testing.T) {
		cookie, err := ctx.Cookie("non-existing")
		if err == nil {
			t.Error("Cookie(non-existing) should return error")
		}
		if cookie != nil {
			t.Error("Cookie(non-existing) should return nil cookie")
		}
	})
}

func TestConnContextJoinRoom(t *testing.T) {
	server := NewServer()
	server.SetLogger(NewNoopLogger())
	ns := server.Of("/test")

	req := httptest.NewRequest("GET", "/test", nil)
	ctx := &ConnContext{
		id:        "conn-123",
		namespace: ns,
		request:   req,
	}

	// Join room
	ctx.JoinRoom("room1")

	// Verify room exists and has connection
	ns.roomsMu.RLock()
	room, exists := ns.rooms["room1"]
	ns.roomsMu.RUnlock()

	if !exists {
		t.Fatal("Room should exist after JoinRoom")
	}

	if !room.has("conn-123") {
		t.Error("Room should contain connection")
	}
}

func TestConnContextLeaveRoom(t *testing.T) {
	server := NewServer()
	server.SetLogger(NewNoopLogger())
	ns := server.Of("/test")

	req := httptest.NewRequest("GET", "/test", nil)
	ctx := &ConnContext{
		id:        "conn-123",
		namespace: ns,
		request:   req,
	}

	// Join then leave
	ctx.JoinRoom("room1")
	ctx.LeaveRoom("room1")

	// Verify room is empty
	ns.roomsMu.RLock()
	_, exists := ns.rooms["room1"]
	ns.roomsMu.RUnlock()

	if exists {
		t.Error("Empty room should be deleted")
	}
}

func TestConnContextRooms(t *testing.T) {
	server := NewServer()
	server.SetLogger(NewNoopLogger())
	ns := server.Of("/test")

	req := httptest.NewRequest("GET", "/test", nil)
	ctx := &ConnContext{
		id:        "conn-123",
		namespace: ns,
		request:   req,
	}

	t.Run("No rooms", func(t *testing.T) {
		rooms := ctx.Rooms()
		if len(rooms) != 0 {
			t.Errorf("Rooms() should return empty slice, got %v", rooms)
		}
	})

	t.Run("Join multiple rooms", func(t *testing.T) {
		ctx.JoinRoom("room1")
		ctx.JoinRoom("room2")
		ctx.JoinRoom("room3")

		rooms := ctx.Rooms()
		if len(rooms) != 3 {
			t.Errorf("Rooms() length = %d, want 3", len(rooms))
		}

		// Check all rooms are present
		roomSet := make(map[string]bool)
		for _, room := range rooms {
			roomSet[room] = true
		}

		expected := []string{"room1", "room2", "room3"}
		for _, expectedRoom := range expected {
			if !roomSet[expectedRoom] {
				t.Errorf("Rooms() missing %s", expectedRoom)
			}
		}
	})

	t.Run("After leaving room", func(t *testing.T) {
		ctx.LeaveRoom("room2")

		rooms := ctx.Rooms()
		if len(rooms) != 2 {
			t.Errorf("Rooms() length = %d, want 2", len(rooms))
		}

		for _, room := range rooms {
			if room == "room2" {
				t.Error("Rooms() should not contain room2 after leaving")
			}
		}
	})
}

func TestConnContextEmit(t *testing.T) {
	server := NewServer()
	server.SetLogger(NewNoopLogger())
	ns := server.Of("/test")

	req := httptest.NewRequest("GET", "/test", nil)

	// Create a mock connection wrapper
	cw := &connWrapper{
		send: make(chan Message, 16),
		ctx:  &ConnContext{id: "conn-123"},
	}

	ns.mu.Lock()
	ns.conns["conn-123"] = cw
	ns.mu.Unlock()

	ctx := &ConnContext{
		id:        "conn-123",
		namespace: ns,
		request:   req,
	}

	t.Run("Emit to existing connection", func(t *testing.T) {
		err := ctx.Emit("test-event", map[string]string{"key": "value"})
		if err != nil {
			t.Errorf("Emit() error = %v", err)
		}

		// Check message was sent
		select {
		case msg := <-cw.send:
			if msg.Event != "test-event" {
				t.Errorf("Message event = %s, want test-event", msg.Event)
			}
		default:
			t.Error("No message received in send channel")
		}
	})

	t.Run("Emit to non-existing connection", func(t *testing.T) {
		ctx2 := &ConnContext{
			id:        "non-existing",
			namespace: ns,
			request:   req,
		}

		err := ctx2.Emit("test", "data")
		if err == nil {
			t.Error("Emit() to non-existing connection should return error")
		}
	})
}

func BenchmarkConnContextLocal(b *testing.B) {
	ctx := &ConnContext{id: "test"}

	b.Run("Set", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			ctx.Local("key", "value")
		}
	})

	b.Run("Get", func(b *testing.B) {
		ctx.Local("key", "value")
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = ctx.Local("key")
		}
	})
}

func BenchmarkConnContextQuery(b *testing.B) {
	req := httptest.NewRequest("GET", "/test?param1=value1&param2=value2", nil)
	ctx := &ConnContext{id: "test", request: req}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = ctx.Query("param1")
	}
}
