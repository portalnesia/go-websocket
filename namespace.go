package websocket

import (
	"context"
	"errors"
	"net/http"
	"sync"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

// Namespace represents a logical boundary for connections and events
type connWrapper struct {
	ws   *websocket.Conn
	send chan Message
	ctx  *ConnContext
	once sync.Once
}

type Namespace struct {
	name string

	server *Server

	mu    sync.RWMutex
	conns map[string]*connWrapper

	roomsMu sync.RWMutex
	rooms   map[string]*Room

	handlersMu   sync.RWMutex
	handlers     map[string]EventHandler
	onConnect    ConnectHandler
	onDisconnect DisconnectHandler
}

// On registers an event handler
func (n *Namespace) On(event string, handler EventHandler) {
	n.handlersMu.Lock()
	defer n.handlersMu.Unlock()

	if n.handlers == nil {
		n.handlers = make(map[string]EventHandler)
	}
	n.handlers[event] = handler
}

// OnConnect sets the connection handler
func (n *Namespace) OnConnect(handler ConnectHandler) {
	n.onConnect = handler
}

// OnDisconnect sets the disconnect handler
func (n *Namespace) OnDisconnect(handler DisconnectHandler) {
	n.onDisconnect = handler
}

// init initializes the namespace
func (n *Namespace) init() {
	// Subscribe to namespace broadcasts
	if n.server.adapter != nil {
		n.server.GetLogger().Debug("Subscribing to adapter", "namespace", n.name)
		n.server.adapter.Subscribe(n.name, func(data BroadcastData) {
			if data.Room != "" {
				n.broadcastToRoomLocal(data.Room, data.Message)
			} else if data.Key != "" && data.Value != nil {
				// Find local connections with matching key-value and emit
				connections := n.FilterConnections(func(ctx *ConnContext) bool {
					return ctx.Local(data.Key) == data.Value
				})
				for _, conn := range connections {
					_ = conn.Emit(data.Message.Message.Event, data.Message.Message.Data)
				}
			} else {
				// Broadcast to local connections
				n.broadcastLocal(data.Message)
			}
		})
	}
}

// getHandler retrieves an event handler
func (n *Namespace) getHandler(event string) (EventHandler, bool) {
	n.handlersMu.RLock()
	defer n.handlersMu.RUnlock()
	handler, ok := n.handlers[event]
	return handler, ok
}

// filterExceptConnIDs checks if connID should be excluded
func filterExceptConnIDs(connID string, exceptIDs []string) bool {
	if len(exceptIDs) == 0 {
		return false
	}
	// Use binary search if sorted, or convert to map for O(1) lookup if len > threshold
	if len(exceptIDs) > 10 {
		// For larger slices, map lookup is more efficient
		exceptMap := make(map[string]struct{}, len(exceptIDs))
		for _, id := range exceptIDs {
			exceptMap[id] = struct{}{}
		}
		_, excluded := exceptMap[connID]
		return excluded
	}
	// For small slices, linear search is fine
	for _, excludeID := range exceptIDs {
		if connID == excludeID {
			return true
		}
	}
	return false
}

// broadcastToConnectionsIf broadcasts to connections that match predicate
// This is a DRY helper to avoid duplication
// Runs in goroutine to avoid blocking caller
func (n *Namespace) broadcastToConnectionsIf(msg Message, predicate func(connID string, cw *connWrapper) bool) {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				n.server.GetLogger().Error("Panic in broadcast", "namespace", n.name, "panic", r)
			}
		}()

		n.mu.RLock()
		defer n.mu.RUnlock()

		for connID, cw := range n.conns {
			if predicate(connID, cw) {
				select {
				case cw.send <- msg:
				default:
					n.server.GetLogger().Warn("Send buffer full", "namespace", n.name, "connID", connID)
				}
			}
		}
	}()
}

func (n *Namespace) addConn(connID string, ws *websocket.Conn, r *http.Request) *ConnContext {
	ctx := &ConnContext{
		id:        connID,
		namespace: n,
		request:   r,
	}

	// Call onConnect handler
	if n.onConnect != nil {
		if err := n.onConnect(ctx); err != nil {
			// Reject connection
			_ = ws.Close(websocket.StatusPolicyViolation, "connection rejected")
			n.server.GetLogger().Debug("Connection rejected", "namespace", n.name, "connID", connID, "error", err.Error())
			return nil
		}
	}

	cw := &connWrapper{
		ws:   ws,
		send: make(chan Message, 16),
		ctx:  ctx, // Store context in wrapper
	}

	n.mu.Lock()
	if n.conns == nil {
		n.conns = make(map[string]*connWrapper)
	}
	n.conns[connID] = cw
	n.mu.Unlock()

	n.server.GetLogger().Debug("Client connected", "namespace", n.name, "connID", connID)

	go n.startWriteLoop(cw)
	go n.startReadLoop(ctx, cw)

	return ctx
}

func (n *Namespace) startWriteLoop(cw *connWrapper) {
	for msg := range cw.send {
		if err := wsjson.Write(context.Background(), cw.ws, msg); err != nil {
			n.server.GetLogger().Error("Write error", "namespace", n.name, "connID", cw.ctx.id, "error", err.Error())
			return
		}
	}
}

func (n *Namespace) startReadLoop(ctx *ConnContext, cw *connWrapper) {
	defer func() {
		n.server.GetLogger().Debug("Client disconnected", "namespace", n.name, "connID", ctx.id)
		n.closeConn(ctx)
	}()

	for {
		var msg Message
		if err := wsjson.Read(context.Background(), cw.ws, &msg); err != nil {
			n.server.GetLogger().Debug("Read error", "namespace", n.name, "connID", ctx.id, "error", err.Error())
			return
		}

		// Dispatch event to handler
		if handler, ok := n.getHandler(msg.Event); ok {
			// Run handler in goroutine to prevent blocking
			go func(h EventHandler, data any, event string) {
				// Recover from panic in handler
				defer func() {
					if r := recover(); r != nil {
						n.server.GetLogger().Error("Panic in event handler", "namespace", n.name, "connID", ctx.id, "event", event, "panic", r)
					}
				}()
				h(ctx, data)
			}(handler, msg.Data, msg.Event)
		}
	}
}

func (n *Namespace) emitToConn(connID string, msg Message) error {
	n.mu.RLock()
	cw, ok := n.conns[connID]
	n.mu.RUnlock()

	if !ok {
		n.server.GetLogger().Debug("Connection not found", "namespace", n.name, "connID", connID)
		return errors.New("connection not found")
	}

	select {
	case cw.send <- msg:
		return nil
	default:
		n.server.GetLogger().Warn("Send buffer full in emitToConn", "namespace", n.name, "connID", connID)
		return errors.New("send buffer full")
	}
}

// Emit broadcasts to all connections
func (n *Namespace) Emit(message Message) {
	nodeMsg := MessageMultiNodes{Message: message}

	// If adapter is set, publish to Redis for multi-node (already async)
	if n.server.adapter != nil {
		n.server.adapter.Broadcast(n.name, BroadcastData{
			Message: nodeMsg,
		})
	}

	// Always broadcast to local connections (runs in goroutine via broadcastToConnectionsIf)
	n.broadcastLocal(nodeMsg)
}

// broadcastLocal broadcasts to local connections only
func (n *Namespace) broadcastLocal(msg MessageMultiNodes) {
	n.broadcastToConnectionsIf(msg.Message, func(connID string, cw *connWrapper) bool {
		// Include if NOT in except list
		return !filterExceptConnIDs(connID, msg.ExceptConnID)
	})
}

// EmitExcept broadcasts to all connections except excludeConnIds
func (n *Namespace) EmitExcept(excludeConnIds []string, message Message) {
	nodeMsg := MessageMultiNodes{
		Message:      message,
		ExceptConnID: excludeConnIds,
	}

	// If adapter is set, publish to Redis for multi-node (already async)
	if n.server.adapter != nil {
		n.server.adapter.Broadcast(n.name, BroadcastData{
			Message: nodeMsg,
		})
	}

	// Always broadcast to local connections (runs in goroutine via broadcastToConnectionsIf)
	n.broadcastLocal(nodeMsg)
}

// emitToLocal sends to specific connection IDs
// Runs in goroutine to avoid blocking caller
func (n *Namespace) emitToLocal(connIDs []string, message Message) {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				n.server.GetLogger().Error("Panic in emitToLocal", "namespace", n.name, "panic", r)
			}
		}()

		n.mu.RLock()
		defer n.mu.RUnlock()

		for _, connID := range connIDs {
			if cw, ok := n.conns[connID]; ok {
				select {
				case cw.send <- message:
				default:
					// Channel full, skip
				}
			}
		}
	}()
}

// EmitToRoom broadcasts to all connections in a room
func (n *Namespace) EmitToRoom(roomName string, message Message) {
	nodeMsg := MessageMultiNodes{
		Message: message,
	}

	// If adapter is set, publish to Redis for multi-node (already async)
	if n.server.adapter != nil {
		n.server.adapter.Broadcast(n.name, BroadcastData{
			Room:    roomName,
			Message: nodeMsg,
		})
	}

	// Always broadcast to local room connections (will run in goroutine)
	n.broadcastToRoomLocal(roomName, nodeMsg)
}

// broadcastToRoomLocal broadcasts to local room connections only
// Runs in goroutine to avoid blocking caller
func (n *Namespace) broadcastToRoomLocal(roomName string, msg MessageMultiNodes) {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				n.server.GetLogger().Error("Panic in broadcastToRoomLocal", "namespace", n.name, "room", roomName, "panic", r)
			}
		}()

		n.roomsMu.RLock()
		room, ok := n.rooms[roomName]
		n.roomsMu.RUnlock()

		if !ok {
			return
		}

		connIDs := room.getConnIDs()

		n.mu.RLock()
		defer n.mu.RUnlock()

		for _, connID := range connIDs {
			// Skip if in except list
			if filterExceptConnIDs(connID, msg.ExceptConnID) {
				continue
			}

			if cw, ok := n.conns[connID]; ok {
				select {
				case cw.send <- msg.Message:
				default:
					// Channel full, skip
				}
			}
		}
	}()
}

// EmitToRoomExcept broadcasts to all connections in a room except excludeConnIds
func (n *Namespace) EmitToRoomExcept(roomName string, excludeConnIds []string, message Message) {
	nodeMsg := MessageMultiNodes{
		Message:      message,
		ExceptConnID: excludeConnIds,
	}

	// If adapter is set, publish to Redis for multi-node (already async)
	if n.server.adapter != nil {
		n.server.adapter.Broadcast(n.name, BroadcastData{
			Room:    roomName,
			Message: nodeMsg,
		})
	}

	// Always broadcast to local room connections (will run in goroutine)
	n.broadcastToRoomLocal(roomName, nodeMsg)
}

// joinRoom adds a connection to a room
func (n *Namespace) joinRoom(connID string, roomName string) {
	n.roomsMu.Lock()
	defer n.roomsMu.Unlock()

	if n.rooms == nil {
		n.rooms = make(map[string]*Room)
	}

	room, ok := n.rooms[roomName]
	if !ok {
		room = newRoom(roomName)
		n.rooms[roomName] = room
		n.server.GetLogger().Debug("Room created", "namespace", n.name, "room", roomName)
	}

	room.add(connID)
	n.server.GetLogger().Debug("Client joined room", "namespace", n.name, "connID", connID, "room", roomName)
}

// leaveRoom removes a connection from a room
func (n *Namespace) leaveRoom(connID string, roomName string) {
	n.roomsMu.Lock()
	defer n.roomsMu.Unlock()

	room, ok := n.rooms[roomName]
	if !ok {
		n.server.GetLogger().Debug("Room not found", "namespace", n.name, "room", roomName)
		return
	}

	// Remove connection from room
	room.remove(connID)
	n.server.GetLogger().Debug("Client left room", "namespace", n.name, "connID", connID, "room", roomName)

	// If room is now empty, delete it
	if room.size() == 0 {
		delete(n.rooms, roomName)
		n.server.GetLogger().Debug("Room deleted (empty)", "namespace", n.name, "room", roomName)
		// Note: Adapter interface does not define UnsubscribeRoom yet
		// Future: n.adapter.UnsubscribeRoom(n.name, roomName)
	}
}

// ConnectionCount returns the number of active connections
func (n *Namespace) ConnectionCount() int {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return len(n.conns)
}

// RoomSize returns the number of connections in a room
func (n *Namespace) RoomSize(roomName string) int {
	n.roomsMu.RLock()
	room, ok := n.rooms[roomName]
	n.roomsMu.RUnlock()

	if !ok {
		return 0
	}

	return room.size()
}

func (n *Namespace) closeConn(ctx *ConnContext) {
	// Get wrapper which contains context first (before removing from rooms)
	n.mu.Lock()
	cw, ok := n.conns[ctx.id]
	if ok {
		delete(n.conns, ctx.id)
		// Clear conns map if empty to prevent memory leak
		if len(n.conns) == 0 {
			n.conns = nil
		}
	}
	n.mu.Unlock()

	if !ok {
		return
	}

	// Remove from all rooms after releasing conns lock
	n.roomsMu.Lock()
	emptyRooms := make([]string, 0)
	for roomName, room := range n.rooms {
		room.remove(ctx.id)
		if room.size() == 0 {
			emptyRooms = append(emptyRooms, roomName)
		}
	}
	// Delete empty rooms
	for _, roomName := range emptyRooms {
		delete(n.rooms, roomName)
	}
	// Clear rooms map if empty to prevent memory leak
	if len(n.rooms) == 0 {
		n.rooms = nil
	}
	n.roomsMu.Unlock()

	// Close connection and cleanup
	cw.once.Do(func() {
		close(cw.send)
		_ = cw.ws.Close(websocket.StatusNormalClosure, "connection closed")
	})

	if n.onDisconnect != nil {
		n.onDisconnect(ctx)
	}
}

// GetAllConnections returns all active ConnContext in the namespace in this nodes
func (n *Namespace) GetAllConnections() []*ConnContext {
	n.mu.RLock()
	contexts := make([]*ConnContext, 0, len(n.conns))
	for _, cw := range n.conns {
		if cw.ctx != nil {
			contexts = append(contexts, cw.ctx)
		}
	}
	n.mu.RUnlock()

	return contexts
}

// GetConnection returns a specific ConnContext by ID in this nodes
func (n *Namespace) GetConnection(connID string) *ConnContext {
	n.mu.RLock()
	defer n.mu.RUnlock()

	if cw, ok := n.conns[connID]; ok {
		return cw.ctx
	}
	return nil
}

// FilterConnections filters connections based on a predicate function in this nodes
func (n *Namespace) FilterConnections(predicate func(*ConnContext) bool) []*ConnContext {
	n.mu.RLock()
	filtered := make([]*ConnContext, 0)
	for _, cw := range n.conns {
		if cw.ctx != nil && predicate(cw.ctx) {
			filtered = append(filtered, cw.ctx)
		}
	}
	n.mu.RUnlock()

	return filtered
}

// FindConnection finds the first connection matching the predicate in this nodes
func (n *Namespace) FindConnection(predicate func(*ConnContext) bool) *ConnContext {
	n.mu.RLock()
	var found *ConnContext
	for _, cw := range n.conns {
		if cw.ctx != nil && predicate(cw.ctx) {
			found = cw.ctx
			break
		}
	}
	n.mu.RUnlock()

	return found
}

// EmitToLocalKeyValue broadcasts to connections with specific local key-value across all nodes
func (n *Namespace) EmitToLocalKeyValue(key string, value any, message Message) {
	msg := MessageMultiNodes{Message: message}

	// If adapter is set, publish to Redis for multi-node (already async)
	if n.server.adapter != nil {
		n.server.adapter.Broadcast(n.name, BroadcastData{
			Message: msg,
			Key:     key,
			Value:   value,
		})
	}

	// Always broadcast to local connections with matching key-value (will run in goroutine)
	n.emitToLocalConnections(key, value, msg)
}

// emitToLocalConnections emits to local connections with matching key-value
func (n *Namespace) emitToLocalConnections(key string, value any, msg MessageMultiNodes) {
	n.broadcastToConnectionsIf(msg.Message, func(connID string, cw *connWrapper) bool {
		// Include if context exists AND local key matches value AND NOT in except list
		return cw.ctx != nil &&
			cw.ctx.Local(key) == value &&
			!filterExceptConnIDs(connID, msg.ExceptConnID)
	})
}

// Close closes all connections and cleans up namespace resources
func (n *Namespace) close() {
	n.server.GetLogger().Info("Closing namespace", "namespace", n.name)
	// Get all connection IDs
	n.mu.Lock()
	connCtx := make([]*ConnContext, 0, len(n.conns))
	for _, conn := range n.conns {
		connCtx = append(connCtx, conn.ctx)
	}
	n.mu.Unlock()

	// Close all connections
	for _, ctx := range connCtx {
		n.closeConn(ctx)
	}

	// Unsubscribe from adapter
	if n.server.adapter != nil {
		n.server.adapter.Unsubscribe(n.name)
		n.server.GetLogger().Debug("Unsubscribed from adapter", "namespace", n.name)
	}

	// Clear handlers
	n.handlersMu.Lock()
	n.handlers = nil
	n.onConnect = nil
	n.onDisconnect = nil
	n.handlersMu.Unlock()
	n.server.GetLogger().Info("Namespace closed", "namespace", n.name)
}
