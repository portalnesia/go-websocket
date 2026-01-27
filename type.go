package websocket

// Message represents the payload structure for client-server communication
type Message struct {
	Event string `json:"event"`
	Data  any    `json:"data"`
}

type MessageMultiNodes struct {
	Message      Message  `json:"message"`
	ExceptConnID []string `json:"except_conn_id,omitempty"`
}

type BroadcastData struct {
	FromServerID string `json:"from_id"`

	Room string `json:"room,omitempty"`

	Key   string `json:"key,omitempty"`
	Value any    `json:"value,omitempty"`

	Message MessageMultiNodes `json:"message,omitempty"`
}

// EventHandler is the function signature for handling events
type EventHandler func(ctx *ConnContext, data any)

// ConnectHandler is called when a new connection is established
// Return error to reject the connection
type ConnectHandler func(ctx *ConnContext) error

// DisconnectHandler is called when a connection is closed
type DisconnectHandler func(ctx *ConnContext)

// Adapter defines the interface for multi-node broadcasting
type Adapter interface {
	Broadcast(namespace string, data BroadcastData)
	Subscribe(namespace string, handler func(data BroadcastData))
	Unsubscribe(namespace string)

	SetLogger(logger Logger)
	Close()
}
