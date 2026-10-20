# Go WebSocket Library

[![Test](https://github.com/portalnesia/go-websocket/actions/workflows/test.yml/badge.svg?branch=main)](https://github.com/portalnesia/go-websocket/actions/workflows/test.yml)

A Go WebSocket library inspired by socket.io, but more explicit, simpler, and doesn't hide the protocol. Clients can use native WebSocket without requiring a specialized SDK.

## Key Features

- ✅ **No Client SDK Required** - Clients only need native WebSocket and JSON
- ✅ **Namespace** - Separate application logic into different namespaces
- ✅ **Event-based Messaging** - Event-based communication with easy handlers
- ✅ **Room Support** - Group connections for focused broadcasts
- ✅ **Connection Lifecycle** - OnConnect and OnDisconnect hooks for full control
- ✅ **Thread-safe** - All operations are safe for concurrent access
- ✅ **Explicit Protocol** - All messages follow a clear contract
- ✅ **Pluggable Logger** - Support for zerolog, zap, logrus, or custom logger
- ✅ **Multi-node Support** - Redis adapter for horizontal scaling

## Installation

```bash
go get go.portalnesia.com/websocket
```

## Quick Start

### Server

```go
package main

import (
    "log"
    websocket "go.portalnesia.com/websocket"
)

func main() {
    // Create server
    server := websocket.NewServer()
    
    // Get or create namespace
    ns := server.Of("/")
    
    // Handle connections
    ns.OnConnect(func(ctx *websocket.ConnContext) error {
        log.Println("Client connected")
        return nil // Return error to reject connection
    })
    
    // Handle events
    ns.On("chat:message", func(ctx *websocket.ConnContext, data any) {
        msgData := data.(map[string]any)
        message := msgData["message"].(string)
        
        // Broadcast to all
        ns.Emit(websocket.Message{
            Event: "chat:message",
            Data: map[string]any{
                "message": message,
            },
        })
    })
    
    // Start server
    log.Fatal(server.Listen(":8080"))
}
```

### Client (JavaScript)

```javascript
// Connect using native WebSocket
const ws = new WebSocket('ws://localhost:8080/pn-socket');

ws.onopen = () => {
    // Send message following the protocol
    ws.send(JSON.stringify({
        event: 'chat:message',
        data: {
            message: 'Hello, World!'
        }
    }));
};

ws.onmessage = (event) => {
    const msg = JSON.parse(event.data);
    console.log('Received:', msg.event, msg.data);
};
```

## Message Protocol

All messages follow this JSON structure:

```json
{
    "event": "event_name",      // Event name (required)
    "data": {                    // Event payload (required, can be any JSON type)
        "key": "value"
    }
}
```

**Note:** Client sends messages to server with structure `{event, data}`. Server will broadcast messages with the same structure.

## Core Concepts

### 1. Server

Entry point for WebSocket server. Manages namespaces and performs HTTP upgrade to WebSocket.

```go
server := websocket.NewServer()

// Custom WebSocket path (default: /pn-socket)
server.SetWSPath("/custom-path")

// The request host is always an allowed origin (same-origin pages, custom domains).
// Patterns add cross-origin callers on top of it.
server.SetOriginPatterns([]string{"example.com", "localhost:3000"})
// Or allow all origins (not recommended for production)
server.SetOriginPatterns([]string{"*"})
// Or use regex patterns
server.SetOriginPatterns([]string{"^https?://.*\\.myapp\\.com$"})

// Set custom logger (optional)
server.SetLogger(websocket.NewDefaultLogger()) // or NewNoopLogger()

// Connection limits (call before serving; values below are the defaults)
server.SetPingInterval(25 * time.Second) // heartbeat ping interval, 0 = heartbeat off
server.SetPongTimeout(10 * time.Second)  // no pong within this time -> connection closed
server.SetWriteTimeout(10 * time.Second) // slower write -> connection closed, 0 = no timeout
server.SetReadLimit(64 << 10)            // larger incoming message -> closed with 1009, <= 0 = unlimited

// Start server
server.Listen(":8080")

// Or integrate with existing HTTP server
http.Handle("/ws", server.Handler())
```

### 2. Namespace

Main logical boundary for separating application concerns. Each namespace has its own event handlers, connection registry, and room registry.

```go
// Default namespace
defaultNS := server.Of("/")

// Custom namespace
chatNS := server.Of("/chat")
notifNS := server.Of("/notifications")

// Client connects with query parameter:
// ws://localhost:8080/pn-socket?namespace=/chat
```

### 3. Connection Context (ConnContext)

Per-connection context that wraps WebSocket connection, HTTP request, and state.

```go
ns.OnConnect(func(ctx *websocket.ConnContext) error {
    // Access HTTP request data
    token := ctx.Query("token")
    
    // Validate and reject if needed
    if !isValid(token) {
        return fmt.Errorf("invalid token")
    }
    
    // Store connection state using Local storage
    ctx.Local("userID", extractUserID(token))
    ctx.Local("username", "John Doe")
    
    return nil
})

ns.On("some:event", func(ctx *websocket.ConnContext, data any) {
    // Get connection state
    userID := ctx.Local("userID")
    
    // Send to this connection
    ctx.Emit("response", map[string]any{"status": "ok"})
})
```

### 4. Events

Register handler for specific events:

```go
ns.On("chat:message", func(ctx *websocket.ConnContext, data any) {
    // Handle the event
    msgData := data.(map[string]any)
    message := msgData["message"].(string)
    username := ctx.Local("username")
    
    // Process and respond
    ns.Emit(websocket.Message{
        Event: "chat:message",
        Data: map[string]any{
            "username": username,
            "message": message,
        },
    })
})
```

### 5. Rooms

Group connections for more focused broadcasts:

```go
ns.On("room:join", func(ctx *websocket.ConnContext, data any) {
    msgData := data.(map[string]any)
    roomName := msgData["room"].(string)
    
    // Join room
    ctx.JoinRoom(roomName)
    
    // Notify room members
    ns.EmitToRoom(roomName, websocket.Message{
        Event: "room:user_joined",
        Data: map[string]any{
            "username": ctx.Local("username"),
        },
    })
})

ns.On("room:leave", func(ctx *websocket.ConnContext, data any) {
    msgData := data.(map[string]any)
    roomName := msgData["room"].(string)
    ctx.LeaveRoom(roomName)
})

ns.On("room:message", func(ctx *websocket.ConnContext, data any) {
    msgData := data.(map[string]any)
    roomName := msgData["room"].(string)
    message := msgData["message"].(string)
    
    // Broadcast to room only
    ns.EmitToRoom(roomName, websocket.Message{
        Event: "room:message",
        Data: map[string]any{
            "room": roomName,
            "message": message,
        },
    })
})
```

### 6. Connection Limits

| Setting | Default constant | Default | Effect |
|---|---|---|---|
| `SetPingInterval` | `DefaultPingInterval` | 25s | Server pings every connection at this interval. `0` disables heartbeat. |
| `SetPongTimeout` | `DefaultPongTimeout` | 10s | No pong within this time closes the connection (`OnDisconnect` runs). |
| `SetWriteTimeout` | `DefaultWriteTimeout` | 10s | A write slower than this (stalled client) closes the connection. `0` = no timeout. |
| `SetReadLimit` | `DefaultReadLimit` | 64 KiB | An incoming message larger than this closes the connection with code `1009`. `<= 0` = unlimited. |

Browsers answer pings automatically, so clients need no code for the heartbeat.

### 7. Broadcast Methods

```go
// 1. Send to single connection
ctx.Emit("event_name", data)

// 2. Broadcast to all connections in namespace
ns.Emit(websocket.Message{Event: "event_name", Data: data})

// 3. Broadcast except sender
ctx.Broadcast(websocket.Message{Event: "event_name", Data: data})

// 4. Broadcast to specific room
ns.EmitToRoom("room_name", websocket.Message{Event: "event_name", Data: data})

// 5. Broadcast to room except sender
ctx.BroadcastToRoom("room_name", websocket.Message{Event: "event_name", Data: data})

// 6. Broadcast to several rooms (one adapter publish; a connection in many of the rooms receives it once)
ns.EmitToRooms([]string{"room_a", "room_b"}, websocket.Message{Event: "event_name", Data: data})
```

### 8. Closing Connections

```go
// Close one connection with an application close code and reason
ctx.CloseWith(4401, "unauthorized")

// Close() is CloseWith(1000, "connection closed")
ctx.Close()

// Close every connection in the rooms, on all nodes
ns.CloseRooms([]string{"user:42"}, 4401, "session revoked")
```

- Valid codes: `1000` and `3000–4999`. **`3000–4999` are reserved for your application**; the library
  never sends them itself (it uses `1000`, `1008` when `OnConnect` rejects, `1009` for a message too big,
  and protocol codes from the WebSocket implementation).
  Any other code is replaced by `1000` and a warning is logged.
- Keep the reason under 123 bytes (WebSocket close frame limit).
- Closing never waits for the network: the connection is removed at once, the close frame and
  `OnDisconnect` run in background. `OnDisconnect` runs exactly once per connection.

The client sees the code in `CloseEvent.code`:

```javascript
ws.onclose = (e) => {
    if (e.code === 4401) redirectToLogin();
};
```

## Authentication & Reconnection

Library provides hooks for implementing auth and reconnect strategy:

```go
ns.OnConnect(func(ctx *websocket.ConnContext) error {
    // Method 1: Query parameter
    token := ctx.Query("token")
    
    // Method 2: Cookie
    cookie, _ := ctx.Cookie("session")
    
    // Method 3: Header
    authHeader := ctx.Header("Authorization")
    
    // Validate
    userID, err := validateAuth(token)
    if err != nil {
        return err // Reject connection
    }
    
    // Store for reconnection
    ctx.Local("userID", userID)
    ctx.Local("sessionID", generateSessionID())
    
    // User can manually rejoin rooms after reconnect
    // Library does NOT auto-restore room membership
    
    return nil
})
```

Client reconnect example:

```javascript
let ws;
let sessionID;

function connect() {
    ws = new WebSocket(`ws://localhost:8080/pn-socket?token=${token}`);
    
    ws.onopen = () => {
        console.log('Connected');
        
        // Manually rejoin rooms after reconnect
        if (sessionID) {
            rejoinRooms();
        }
    };
    
    ws.onclose = () => {
        console.log('Disconnected');
        // Implement reconnect logic
        setTimeout(connect, 3000);
    };
}

function rejoinRooms() {
    // User is responsible for rejoining rooms
    // Library only provides the hooks
    ws.send(JSON.stringify({
        event: 'room:join',
        data: { room: 'general' }
    }));
}
```

## API Reference

### Server

```go
const (
    DefaultPingInterval = 25 * time.Second
    DefaultPongTimeout  = 10 * time.Second
    DefaultWriteTimeout = 10 * time.Second
    DefaultReadLimit    = 64 << 10 // bytes
)

func NewServer() *Server
func NewFiberServer() *Server
func (s *Server) SetWSPath(path string)
func (s *Server) SetPingInterval(d time.Duration)
func (s *Server) SetPongTimeout(d time.Duration)
func (s *Server) SetWriteTimeout(d time.Duration)
func (s *Server) SetReadLimit(n int64)
func (s *Server) SetOriginPatterns(patterns []string)
func (s *Server) SetAdapter(adapter Adapter)
func (s *Server) SetLogger(logger Logger)
func (s *Server) GetLogger() Logger
func (s *Server) Of(name string) *Namespace
func (s *Server) Listen(addr string) error
func (s *Server) Handler() http.Handler
func (s *Server) Close()
```

### Namespace

```go
func (ns *Namespace) On(event string, handler EventHandler)
func (ns *Namespace) OnConnect(handler ConnectHandler)
func (ns *Namespace) OnDisconnect(handler DisconnectHandler)
func (ns *Namespace) Emit(msg Message)
func (ns *Namespace) EmitExcept(excludeConnIds []string, msg Message)
func (ns *Namespace) EmitToRoom(roomName string, msg Message)
func (ns *Namespace) EmitToRoomExcept(roomName string, excludeConnIds []string, msg Message)
func (ns *Namespace) EmitToRooms(rooms []string, msg Message)
func (ns *Namespace) CloseRooms(rooms []string, code int, reason string)
func (ns *Namespace) ConnectionCount() int
func (ns *Namespace) RoomSize(roomName string) int
func (ns *Namespace) GetAllConnections() []*ConnContext
func (ns *Namespace) GetConnection(connID string) *ConnContext
func (ns *Namespace) FilterConnections(predicate func(*ConnContext) bool) []*ConnContext
```

### ConnContext

```go
func (c *ConnContext) ID() string
func (c *ConnContext) Local(key string, value ...any) any
func (c *ConnContext) Header(key string) string
func (c *ConnContext) Query(key string) string
func (c *ConnContext) Cookie(name string) (*http.Cookie, error)
func (c *ConnContext) RequestURL() *url.URL
func (c *ConnContext) Emit(event string, data any) error
func (c *ConnContext) Broadcast(msg Message)
func (c *ConnContext) BroadcastToRoom(roomName string, msg Message)
func (c *ConnContext) JoinRoom(roomName string)
func (c *ConnContext) LeaveRoom(roomName string)
func (c *ConnContext) Rooms() []string
func (c *ConnContext) Close()
func (c *ConnContext) CloseWith(code int, reason string)
```

## Examples

See `examples/` folder for complete examples:

- `examples/basic/` - Chat application with room support
- `examples/basic/client.html` - HTML client demo

### Running the Example

```bash
cd examples/basic
go run main.go
```

Open `client.html` in browser for testing.

## Multi-node Support

Redis adapter for horizontal scaling and multi-node deployment:

```go
import (
    "github.com/redis/go-redis/v9"
    websocket "go.portalnesia.com/websocket"
    redisAdapter "go.portalnesia.com/websocket/adapter/redis"
)

func main() {
    // Any redis.UniversalClient works: *redis.Client, cluster, sentinel/failover
    redisClient := redis.NewUniversalClient(&redis.UniversalOptions{
        Addrs: []string{"localhost:6379"},
    })

    // Create Redis adapter with optional config
    adapter, err := redisAdapter.New(redisClient, &redisAdapter.Config{
        MaxPublishGoros: 100,  // Max concurrent publish goroutines
        ErrorCallback: func(err error) {
            log.Printf("Redis adapter error: %v", err)
        },
    })
    if err != nil {
        log.Fatal(err)
    }
    defer adapter.Close()

    // Create server and set adapter
    server := websocket.NewServer()
    server.SetAdapter(adapter)

    ns := server.Of("/")
    
    // Now broadcasts will work across all nodes
    ns.Emit(message) // Reaches all nodes via Redis pub/sub
    ns.EmitToRoom("room", message) // Reaches all nodes
    ns.EmitToRooms([]string{"a", "b"}, message) // One publish for all rooms
    ns.CloseRooms([]string{"a"}, 4000, "closed") // Closes connections in room "a" on all nodes
}
```

### How it Works

1. **Pub/Sub Architecture**: Redis adapter uses Redis Pub/Sub to broadcast messages between nodes
2. **Namespace Channels**: Each namespace has a dedicated Redis channel
3. **Automatic Subscription**: Adapter automatically subscribes to namespace channels
4. **Bounded Concurrency**: Publish operations are limited to prevent resource exhaustion
5. **Graceful Shutdown**: Waits for pending operations during close
6. **At-most-once**: Messages published while a node or Redis is disconnected are lost; nothing is stored or resent
7. **Rooms are per node**: Membership is not synced; room emits and closes are published and each node applies them locally
8. **Mixed versions**: Payloads from `EmitToRooms`/`CloseRooms` carry a guard so nodes running an older version
   ignore them instead of broadcasting to everyone. Those older nodes do not deliver them, so upgrade every node
   before relying on these methods.

### Logger Support

Server and adapter support pluggable logger:

```go
import (
    "github.com/rs/zerolog"
    websocket "go.portalnesia.com/websocket"
    zerologAdapter "go.portalnesia.com/websocket/logger/zerolog"
)

// Use zerolog
zlog := zerolog.New(os.Stdout).With().Timestamp().Logger()
server.SetLogger(zerologAdapter.New(zlog))

// Or use default logger
server.SetLogger(websocket.NewDefaultLogger())

// Or disable logging
server.SetLogger(websocket.NewNoopLogger())
```

Supported loggers:
- `zerolog` - High performance structured logger
- `zap` - Uber's fast structured logger
- `logrus` - Popular structured logger
- Custom - Implement `Logger` interface

## Limitations

This MVP does **NOT** include:

- ❌ Client SDK - Use native WebSocket
- ❌ Acknowledgement / Request-Response pattern
- ❌ Middleware per-message
- ❌ Presence state sync
- ❌ Guaranteed delivery
- ❌ Exactly-once semantics

These features can be built on top of this core engine as needed.

## Design Principles

1. **NO CLIENT SDK** - Library doesn't depend on complex client logic
2. **CORE ENGINE > FEATURES** - Focus on solid core, advanced features can be built on top
3. **EXPLICIT PROTOCOL** - No "magic" messages, everything follows a clear contract

## Contributing

Contributions are welcome! Please read the architecture document for design principles.

## License

MIT License - see [LICENSE](LICENSE) file for details

## Credits

Inspired by socket.io with a more explicit and simpler approach.
