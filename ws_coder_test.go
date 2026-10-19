package websocket

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// serveCoder runs handler with a wrapped server-side conn and returns a dialed client
func serveCoder(t *testing.T, writeTimeout time.Duration, handler func(WSConn)) *websocket.Conn {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		handler(newCoderWSConn(c, writeTimeout))
	}))
	t.Cleanup(srv.Close)

	client, _, err := websocket.Dial(context.Background(), "ws"+strings.TrimPrefix(srv.URL, "http"), nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = client.CloseNow() })
	return client
}

// writeUntilError writes 1MB messages to a peer that never reads
func writeUntilError(ws WSConn) error {
	big := strings.Repeat("x", 1<<20)
	for i := 0; i < 500; i++ {
		if err := ws.WriteJSON(Message{Event: "big", Data: big}); err != nil {
			return err
		}
	}
	return nil
}

// pingWithReader pings while a reader processes control frames, then closes and waits for the reader
func pingWithReader(ws WSConn, timeout time.Duration) error {
	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		_ = ws.ReadJSON(&Message{})
	}()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	err := ws.Ping(ctx)
	_ = ws.Close(1000, "")
	<-readDone
	return err
}

func TestCoderWriteTimeout(t *testing.T) {
	errc := make(chan error, 1)
	serveCoder(t, 50*time.Millisecond, func(ws WSConn) { errc <- writeUntilError(ws) })

	select {
	case err := <-errc:
		if err == nil {
			t.Fatal("WriteJSON to a stalled peer should fail after write timeout")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("WriteJSON blocked past write timeout")
	}
}

func TestCoderPing(t *testing.T) {
	t.Run("pong received", func(t *testing.T) {
		errc := make(chan error, 1)
		client := serveCoder(t, time.Second, func(ws WSConn) {
			errc <- pingWithReader(ws, time.Second)
		})
		client.CloseRead(context.Background()) // answers pings
		if err := <-errc; err != nil {
			t.Errorf("Ping() = %v, want nil", err)
		}
	})

	t.Run("no pong", func(t *testing.T) {
		errc := make(chan error, 1)
		serveCoder(t, time.Second, func(ws WSConn) {
			errc <- pingWithReader(ws, 50*time.Millisecond)
		})
		// client never reads, so it never answers the ping
		if err := <-errc; err == nil {
			t.Error("Ping() without pong should fail")
		}
	})
}
