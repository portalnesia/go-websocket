package websocket

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/gofiber/fiber/v2"
	fiberws "github.com/gofiber/websocket/v2"
)

// startFiber serves app on a local listener and returns its ws:// base URL
func startFiber(t *testing.T, app *fiber.App) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() { _ = app.Listener(ln) }()
	t.Cleanup(func() { _ = app.ShutdownWithTimeout(time.Second) })
	return "ws://" + ln.Addr().String()
}

func dialWS(t *testing.T, url string) *websocket.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		t.Fatalf("dial %s: %v", url, err)
	}
	t.Cleanup(func() { _ = c.CloseNow() })
	return c
}

// serveFiber runs handler with a wrapped server-side conn and returns a dialed client
func serveFiber(t *testing.T, writeTimeout time.Duration, handler func(WSConn)) *websocket.Conn {
	t.Helper()
	app := fiber.New(fiber.Config{DisableStartupMessage: true})
	app.Get("/ws", fiberws.New(func(c *fiberws.Conn) {
		handler(newFiberWSConn(c, writeTimeout, 0))
	}))
	return dialWS(t, startFiber(t, app)+"/ws")
}

func TestFiberWriteTimeout(t *testing.T) {
	errc := make(chan error, 1)
	serveFiber(t, 50*time.Millisecond, func(ws WSConn) { errc <- writeUntilError(ws) })

	select {
	case err := <-errc:
		if err == nil {
			t.Fatal("WriteJSON to a stalled peer should fail after write timeout")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("WriteJSON blocked past write timeout")
	}
}

func TestFiberPing(t *testing.T) {
	t.Run("pong received", func(t *testing.T) {
		errc := make(chan error, 1)
		client := serveFiber(t, time.Second, func(ws WSConn) {
			errc <- pingWithReader(ws, time.Second)
		})
		client.CloseRead(context.Background()) // answers pings
		if err := <-errc; err != nil {
			t.Errorf("Ping() = %v, want nil", err)
		}
	})

	t.Run("no pong", func(t *testing.T) {
		errc := make(chan error, 1)
		serveFiber(t, time.Second, func(ws WSConn) {
			errc <- pingWithReader(ws, 50*time.Millisecond)
		})
		if err := <-errc; err == nil {
			t.Error("Ping() without pong should fail")
		}
	})
}
