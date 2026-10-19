package websocket

import (
	"context"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

// coderWSConn wraps github.com/coder/websocket connection
type coderWSConn struct {
	conn         *websocket.Conn
	writeTimeout time.Duration
}

func newCoderWSConn(conn *websocket.Conn, writeTimeout time.Duration) WSConn {
	return &coderWSConn{conn: conn, writeTimeout: writeTimeout}
}

func (c *coderWSConn) ReadJSON(v interface{}) error {
	return wsjson.Read(context.Background(), c.conn, v)
}

func (c *coderWSConn) WriteJSON(v interface{}) error {
	ctx := context.Background()
	if c.writeTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.writeTimeout)
		defer cancel()
	}
	return wsjson.Write(ctx, c.conn, v)
}

func (c *coderWSConn) Close(code int, reason string) error {
	return c.conn.Close(websocket.StatusCode(code), reason)
}

func (c *coderWSConn) Ping(ctx context.Context) error {
	return c.conn.Ping(ctx)
}

func (c *coderWSConn) SetReadLimit(n int64) {
	if n <= 0 {
		n = -1 // coder: -1 disables the limit
	}
	c.conn.SetReadLimit(n)
}
