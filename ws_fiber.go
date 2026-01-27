package websocket

import (
	fiberws "github.com/gofiber/websocket/v2"
)

// fiberWSConn wraps github.com/gofiber/websocket/v2 connection
type fiberWSConn struct {
	conn *fiberws.Conn
}

func newFiberWSConn(conn *fiberws.Conn) WSConn {
	return &fiberWSConn{conn: conn}
}

func (c *fiberWSConn) ReadJSON(v interface{}) error {
	return c.conn.ReadJSON(v)
}

func (c *fiberWSConn) WriteJSON(v interface{}) error {
	return c.conn.WriteJSON(v)
}

func (c *fiberWSConn) Close(code int, reason string) error {
	return c.conn.Close()
}
