package websocket

import (
	"sync"
	"testing"
)

func TestNewRoom(t *testing.T) {
	roomName := "test-room"
	room := newRoom(roomName)

	if room == nil {
		t.Fatal("newRoom() returned nil")
	}

	if room.name != roomName {
		t.Errorf("room.name = %s, want %s", room.name, roomName)
	}

	if room.conns == nil {
		t.Error("room.conns should not be nil")
	}

	if len(room.conns) != 0 {
		t.Errorf("new room should be empty, got %d connections", len(room.conns))
	}
}

func TestRoomAdd(t *testing.T) {
	room := newRoom("test")

	// Add first connection
	room.add("conn1")
	if room.size() != 1 {
		t.Errorf("room.size() = %d, want 1", room.size())
	}

	if !room.has("conn1") {
		t.Error("room should have conn1")
	}

	// Add second connection
	room.add("conn2")
	if room.size() != 2 {
		t.Errorf("room.size() = %d, want 2", room.size())
	}

	// Add duplicate (should not increase size)
	room.add("conn1")
	if room.size() != 2 {
		t.Errorf("adding duplicate should not increase size, got %d, want 2", room.size())
	}
}

func TestRoomRemove(t *testing.T) {
	room := newRoom("test")
	room.add("conn1")
	room.add("conn2")
	room.add("conn3")

	// Remove existing connection
	room.remove("conn2")
	if room.size() != 2 {
		t.Errorf("room.size() = %d, want 2", room.size())
	}

	if room.has("conn2") {
		t.Error("room should not have conn2 after removal")
	}

	// Remove non-existing connection (should not panic)
	room.remove("conn999")
	if room.size() != 2 {
		t.Errorf("removing non-existing conn should not change size, got %d, want 2", room.size())
	}

	// Remove remaining connections
	room.remove("conn1")
	room.remove("conn3")
	if room.size() != 0 {
		t.Errorf("room.size() = %d, want 0", room.size())
	}
}

func TestRoomHas(t *testing.T) {
	room := newRoom("test")

	// Empty room
	if room.has("conn1") {
		t.Error("empty room should not have any connections")
	}

	// After adding
	room.add("conn1")
	if !room.has("conn1") {
		t.Error("room should have conn1")
	}

	// After removing
	room.remove("conn1")
	if room.has("conn1") {
		t.Error("room should not have conn1 after removal")
	}
}

func TestRoomSize(t *testing.T) {
	room := newRoom("test")

	// Empty room
	if room.size() != 0 {
		t.Errorf("empty room size = %d, want 0", room.size())
	}

	// Add connections
	for i := 1; i <= 10; i++ {
		room.add(generateId())
		if room.size() != i {
			t.Errorf("room.size() = %d, want %d", room.size(), i)
		}
	}
}

func TestRoomGetConnIDs(t *testing.T) {
	room := newRoom("test")

	// Empty room
	ids := room.getConnIDs()
	if len(ids) != 0 {
		t.Errorf("empty room getConnIDs() length = %d, want 0", len(ids))
	}

	// Add connections
	connIDs := []string{"conn1", "conn2", "conn3"}
	for _, id := range connIDs {
		room.add(id)
	}

	// Get IDs
	retrievedIDs := room.getConnIDs()
	if len(retrievedIDs) != len(connIDs) {
		t.Errorf("getConnIDs() length = %d, want %d", len(retrievedIDs), len(connIDs))
	}

	// Check all IDs are present
	idMap := make(map[string]bool)
	for _, id := range retrievedIDs {
		idMap[id] = true
	}

	for _, expectedID := range connIDs {
		if !idMap[expectedID] {
			t.Errorf("getConnIDs() missing %s", expectedID)
		}
	}

	// Verify modifying returned slice doesn't affect room
	if len(retrievedIDs) > 0 {
		originalSize := room.size()
		retrievedIDs[0] = "modified"
		if room.size() != originalSize {
			t.Error("modifying returned IDs should not affect room")
		}
	}
}

func TestRoomConcurrency(t *testing.T) {
	room := newRoom("test")
	const goroutines = 50
	const opsPerGoroutine = 100

	wg := sync.WaitGroup{}
	wg.Add(goroutines)

	// Concurrent adds
	for i := 0; i < goroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				connID := generateId()
				room.add(connID)
			}
		}(i)
	}

	wg.Wait()

	// Check final size
	finalSize := room.size()
	if finalSize != goroutines*opsPerGoroutine {
		t.Errorf("room.size() = %d, want %d", finalSize, goroutines*opsPerGoroutine)
	}

	// Concurrent reads and writes
	wg.Add(goroutines * 3)

	// Add
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				room.add(generateId())
			}
		}()
	}

	// Check
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				_ = room.has("conn1")
				_ = room.size()
			}
		}()
	}

	// Get IDs
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				_ = room.getConnIDs()
			}
		}()
	}

	wg.Wait()
}

func TestRoomRemoveConcurrency(t *testing.T) {
	room := newRoom("test")

	// Add many connections
	connIDs := make([]string, 1000)
	for i := 0; i < 1000; i++ {
		connIDs[i] = generateId()
		room.add(connIDs[i])
	}

	// Concurrent removes
	wg := sync.WaitGroup{}
	wg.Add(len(connIDs))

	for _, id := range connIDs {
		go func(connID string) {
			defer wg.Done()
			room.remove(connID)
		}(id)
	}

	wg.Wait()

	// Should be empty
	if room.size() != 0 {
		t.Errorf("room.size() = %d, want 0 after removing all connections", room.size())
	}
}

func BenchmarkRoomAdd(b *testing.B) {
	room := newRoom("test")
	ids := make([]string, b.N)
	for i := 0; i < b.N; i++ {
		ids[i] = generateId()
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		room.add(ids[i])
	}
}

func BenchmarkRoomHas(b *testing.B) {
	room := newRoom("test")
	for i := 0; i < 1000; i++ {
		room.add(generateId())
	}

	id := generateId()
	room.add(id)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = room.has(id)
	}
}

func BenchmarkRoomGetConnIDs(b *testing.B) {
	room := newRoom("test")
	for i := 0; i < 1000; i++ {
		room.add(generateId())
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = room.getConnIDs()
	}
}
