package websocket

import (
	"net/http"
	"net/url"
	"sync"
)

// ConnContext represents a single WebSocket connection context
type ConnContext struct {
	id        string
	namespace *Namespace
	request   *http.Request
	local     map[string]any
	localMu   sync.RWMutex
	done      chan struct{}
}

// ID returns the connection ID
func (c *ConnContext) ID() string {
	return c.id
}

// Local get or sets a local value for the connection context
// if value undefined, it will get
// if value is nil, it will remove the key
// if value is defined, it will set
func (c *ConnContext) Local(key string, value ...any) any {
	c.localMu.Lock()
	defer c.localMu.Unlock()

	if c.local == nil {
		c.local = make(map[string]any)
	}

	// Get operation
	if len(value) == 0 {
		return c.local[key]
	}

	// Set or delete operation
	if value[0] == nil {
		delete(c.local, key)
		// Clear map if empty to prevent memory leak
		if len(c.local) == 0 {
			c.local = nil
		}
		return nil
	}

	c.local[key] = value[0]

	return c.local[key]
}

// Header retrieves a header value from the connection request
func (c *ConnContext) Header(key string) string {
	return c.request.Header.Get(key)
}

func (c *ConnContext) RequestURL() *url.URL {
	return c.request.URL
}

// Query retrieves a query parameter from the connection request
func (c *ConnContext) Query(key string) string {
	return c.request.URL.Query().Get(key)
}

// Cookie retrieves a cookie from the connection request
func (c *ConnContext) Cookie(name string) (*http.Cookie, error) {
	return c.request.Cookie(name)
}

func (c *ConnContext) Emit(event string, data any) error {
	err := c.namespace.emitToConn(c.id, Message{
		Event: event,
		Data:  data,
	})
	if err != nil {
		c.namespace.server.GetLogger().Debug("Emit failed", "connID", c.id, "event", event, "error", err.Error())
	}
	return err
}

func (c *ConnContext) Broadcast(message Message) {
	c.namespace.EmitExcept([]string{c.id}, message)
}

func (c *ConnContext) BroadcastToRoom(roomName string, message Message) {
	c.namespace.EmitToRoomExcept(roomName, []string{c.id}, message)
}

// JoinRoom adds the connection to a room
func (c *ConnContext) JoinRoom(roomName string) {
	c.namespace.joinRoom(c.id, roomName)
}

// LeaveRoom removes the connection from a room
func (c *ConnContext) LeaveRoom(roomName string) {
	c.namespace.leaveRoom(c.id, roomName)
}

// Rooms returns the list of rooms the connection is in
func (c *ConnContext) Rooms() []string {
	c.namespace.roomsMu.RLock()
	// Copy room references while holding lock
	roomsCopy := make([]*Room, 0, len(c.namespace.rooms))
	roomNames := make([]string, 0, len(c.namespace.rooms))
	for roomName, room := range c.namespace.rooms {
		roomsCopy = append(roomsCopy, room)
		roomNames = append(roomNames, roomName)
	}
	c.namespace.roomsMu.RUnlock()

	// Now check membership without holding namespace lock
	result := make([]string, 0)
	for i, room := range roomsCopy {
		if room.has(c.id) {
			result = append(result, roomNames[i])
		}
	}
	return result
}

// Close closes the connection with code 1000; same as CloseWith(1000, "connection closed")
func (c *ConnContext) Close() {
	c.CloseWith(1000, "connection closed")
}

// CloseWith closes the connection with code and reason sent in the close frame.
// Valid codes are 1000 and 3000-4999; others are replaced by 1000.
// The connection is removed immediately; the close frame and OnDisconnect run in background.
func (c *ConnContext) CloseWith(code int, reason string) {
	c.namespace.closeConnAsync(c, c.namespace.closeCode(code), reason)
}

// Done returns a channel that's closed when the connection is closed
func (c *ConnContext) Done() <-chan struct{} {
	return c.done
}
