package websocket

import (
	"context"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

// coderWSConn wraps github.com/coder/websocket connection
type coderWSConn struct {
	conn *websocket.Conn
}

func newCoderWSConn(conn *websocket.Conn) WSConn {
	return &coderWSConn{conn: conn}
}

func (c *coderWSConn) ReadJSON(v interface{}) error {
	return wsjson.Read(context.Background(), c.conn, v)
}

func (c *coderWSConn) WriteJSON(v interface{}) error {
	return wsjson.Write(context.Background(), c.conn, v)
}

func (c *coderWSConn) Close(code int, reason string) error {
	return c.conn.Close(websocket.StatusCode(code), reason)
}
