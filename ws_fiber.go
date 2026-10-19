package websocket

import (
	"context"
	"net"
	"sync"
	"time"

	fiberws "github.com/gofiber/websocket/v2"
)

// fiberWSConn wraps github.com/gofiber/websocket/v2 connection
type fiberWSConn struct {
	conn         *fiberws.Conn
	writeTimeout time.Duration
	pong         chan struct{}
	closed       chan struct{}
	closeOnce    sync.Once
}

// newFiberWSConn wraps conn. readWindow > 0 sets a read deadline that every pong extends.
func newFiberWSConn(conn *fiberws.Conn, writeTimeout, readWindow time.Duration) WSConn {
	c := &fiberWSConn{
		conn:         conn,
		writeTimeout: writeTimeout,
		pong:         make(chan struct{}, 1),
		closed:       make(chan struct{}),
	}
	if readWindow > 0 {
		_ = conn.SetReadDeadline(time.Now().Add(readWindow))
	}
	conn.SetPongHandler(func(string) error {
		if readWindow > 0 {
			_ = conn.SetReadDeadline(time.Now().Add(readWindow))
		}
		select {
		case c.pong <- struct{}{}:
		default:
		}
		return nil
	})
	return c
}

// writeDeadline returns zero (no deadline) when writeTimeout is disabled
func (c *fiberWSConn) writeDeadline() time.Time {
	if c.writeTimeout <= 0 {
		return time.Time{}
	}
	return time.Now().Add(c.writeTimeout)
}

func (c *fiberWSConn) ReadJSON(v interface{}) error {
	return c.conn.ReadJSON(v)
}

func (c *fiberWSConn) WriteJSON(v interface{}) error {
	_ = c.conn.SetWriteDeadline(c.writeDeadline())
	return c.conn.WriteJSON(v)
}

func (c *fiberWSConn) Close(code int, reason string) error {
	c.closeOnce.Do(func() { close(c.closed) })
	_ = c.conn.WriteControl(fiberws.CloseMessage, fiberws.FormatCloseMessage(code, reason), c.writeDeadline())
	// fasthttp keeps a hijacked socket open until the handler returns; unblock ReadJSON explicitly
	_ = c.conn.SetReadDeadline(time.Now())
	return c.conn.Close()
}

// Ping relies on the read loop to process the pong (pong handler runs inside ReadJSON)
func (c *fiberWSConn) Ping(ctx context.Context) error {
	select {
	case <-c.pong: // drop a stale pong
	default:
	}
	deadline, _ := ctx.Deadline()
	if err := c.conn.WriteControl(fiberws.PingMessage, nil, deadline); err != nil {
		return err
	}
	select {
	case <-c.pong:
		return nil
	case <-c.closed:
		return net.ErrClosed
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *fiberWSConn) SetReadLimit(n int64) {
	if n < 0 {
		n = 0 // fasthttp: 0 disables the limit
	}
	c.conn.SetReadLimit(n)
}
