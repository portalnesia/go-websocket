package websocket

import "context"

// WSConn is an interface that abstracts WebSocket connection
// This allows us to support multiple WebSocket libraries
type WSConn interface {
	// ReadJSON reads a JSON message from the connection
	ReadJSON(v interface{}) error

	// WriteJSON writes a JSON message to the connection.
	// Implementations must fail once the wrapper's write timeout is exceeded.
	WriteJSON(v interface{}) error

	// Close sends a close frame with status code and reason, then closes the connection
	Close(code int, reason string) error

	// Ping sends a ping and blocks until the pong arrives or ctx is done
	Ping(ctx context.Context) error

	// SetReadLimit sets the max size in bytes of an incoming message (<= 0 = unlimited).
	// Exceeding it closes the connection with status 1009.
	SetReadLimit(n int64)
}
