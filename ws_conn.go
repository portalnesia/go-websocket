package websocket

// WSConn is an interface that abstracts WebSocket connection
// This allows us to support multiple WebSocket libraries
type WSConn interface {
	// ReadJSON reads a JSON message from the connection
	ReadJSON(v interface{}) error

	// WriteJSON writes a JSON message to the connection
	WriteJSON(v interface{}) error

	// Close closes the connection with status code and reason
	Close(code int, reason string) error
}
