package websocket

import "sync"

// Room represents a group of connections
type Room struct {
	name  string
	conns map[string]struct{} // connID set
	mu    sync.RWMutex
}

func newRoom(name string) *Room {
	return &Room{
		name:  name,
		conns: make(map[string]struct{}),
	}
}

// Add adds a connection to the room
func (r *Room) add(connID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.conns[connID] = struct{}{}
}

// Remove removes a connection from the room
func (r *Room) remove(connID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.conns, connID)
}

// Has checks if a connection is in the room
func (r *Room) has(connID string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.conns[connID]
	return ok
}

// Size returns the number of connections in the room
func (r *Room) size() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.conns)
}

// GetConnIDs returns all connection IDs in the room
func (r *Room) getConnIDs() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	ids := make([]string, 0, len(r.conns))
	for id := range r.conns {
		ids = append(ids, id)
	}
	return ids
}
