package websocket

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

func TestNewServer(t *testing.T) {
	server := NewServer()

	if server == nil {
		t.Fatal("NewServer() should not return nil")
	}

	if server.namespaces == nil {
		t.Error("namespaces map should be initialized")
	}

	if server.wsPath != DefaultWSPath {
		t.Errorf("wsPath = %s, want %s", server.wsPath, DefaultWSPath)
	}

	if server.logger == nil {
		t.Error("logger should be initialized")
	}

	if server.allowAllOrigins {
		t.Error("allowAllOrigins should be false by default")
	}
}

func TestServerSetAdapter(t *testing.T) {
	server := NewServer()
	server.SetLogger(NewNoopLogger())

	// Mock adapter
	mockAdapter := &mockAdapter{}
	server.SetAdapter(mockAdapter)

	if server.adapter == nil {
		t.Error("adapter should be set")
	}

	if !mockAdapter.loggerSet {
		t.Error("SetAdapter should propagate logger to adapter")
	}
}

func TestServerSetLogger(t *testing.T) {
	server := NewServer()

	t.Run("Set custom logger", func(t *testing.T) {
		customLogger := NewNoopLogger()
		server.SetLogger(customLogger)

		if server.logger == nil {
			t.Error("logger should be set")
		}
	})

	t.Run("Set nil logger", func(t *testing.T) {
		server.SetLogger(nil)
		logger := server.GetLogger()

		if logger == nil {
			t.Error("GetLogger() should never return nil")
		}
	})
}

func TestServerGetLogger(t *testing.T) {
	server := NewServer()

	logger := server.GetLogger()
	if logger == nil {
		t.Fatal("GetLogger() should never return nil")
	}

	// Test with nil logger
	server.logger = nil
	logger = server.GetLogger()
	if logger == nil {
		t.Error("GetLogger() should return NoopLogger when logger is nil")
	}
}

func TestServerOf(t *testing.T) {
	server := NewServer()
	server.SetLogger(NewNoopLogger())

	t.Run("Create new namespace", func(t *testing.T) {
		ns := server.Of("/chat")

		if ns == nil {
			t.Fatal("Of() should not return nil")
		}

		if ns.name != "/chat" {
			t.Errorf("namespace name = %s, want /chat", ns.name)
		}

		if ns.server != server {
			t.Error("namespace should reference server")
		}
	})

	t.Run("Get existing namespace", func(t *testing.T) {
		ns1 := server.Of("/existing")
		ns2 := server.Of("/existing")

		if ns1 != ns2 {
			t.Error("Of() should return same namespace instance")
		}
	})

	t.Run("Multiple namespaces", func(t *testing.T) {
		ns1 := server.Of("/ns1")
		ns2 := server.Of("/ns2")
		ns3 := server.Of("/ns3")

		if ns1 == ns2 || ns2 == ns3 || ns1 == ns3 {
			t.Error("Different namespaces should be different instances")
		}

		if len(server.namespaces) < 3 {
			t.Errorf("server should have at least 3 namespaces, got %d", len(server.namespaces))
		}
	})
}

func TestServerSetWSPath(t *testing.T) {
	server := NewServer()

	server.SetWSPath("/custom-path")

	if server.wsPath != "/custom-path" {
		t.Errorf("wsPath = %s, want /custom-path", server.wsPath)
	}
}

func TestServerSetOriginPatterns(t *testing.T) {
	server := NewServer()

	t.Run("Allow all origins with *", func(t *testing.T) {
		server.SetOriginPatterns([]string{"*"})

		if !server.allowAllOrigins {
			t.Error("allowAllOrigins should be true with * pattern")
		}
	})

	t.Run("Specific patterns", func(t *testing.T) {
		server.SetOriginPatterns([]string{"example.com", "localhost:3000"})

		if server.allowAllOrigins {
			t.Error("allowAllOrigins should be false with specific patterns")
		}

		if len(server.originPatterns) != 2 {
			t.Errorf("originPatterns length = %d, want 2", len(server.originPatterns))
		}
	})

	t.Run("Regex patterns", func(t *testing.T) {
		server.SetOriginPatterns([]string{"^https?://.*\\.example\\.com$"})

		if len(server.originRegexps) != 1 {
			t.Errorf("originRegexps length = %d, want 1", len(server.originRegexps))
		}
	})
}

func TestServerCheckOrigin(t *testing.T) {
	server := NewServer()

	t.Run("Allow all origins", func(t *testing.T) {
		server.SetOriginPatterns([]string{"*"})

		if !server.checkOrigin("http://any-origin.com", "example.com") {
			t.Error("Should allow any origin with * pattern")
		}
	})

	t.Run("Default - match request host", func(t *testing.T) {
		server.SetOriginPatterns([]string{})

		if !server.checkOrigin("https://example.com", "example.com") {
			t.Error("Should allow origin matching request host")
		}

		if !server.checkOrigin("http://example.com", "example.com") {
			t.Error("Should allow both http and https")
		}

		if server.checkOrigin("https://other.com", "example.com") {
			t.Error("Should not allow different origin")
		}
	})

	t.Run("Empty origin", func(t *testing.T) {
		server.SetOriginPatterns([]string{})

		if server.checkOrigin("", "example.com") {
			t.Error("Empty origin should not be allowed")
		}
	})

	t.Run("Exact patterns", func(t *testing.T) {
		server.SetOriginPatterns([]string{"example.com", "localhost:3000"})

		if !server.checkOrigin("https://example.com", "any-host") {
			t.Error("Should allow https://example.com")
		}

		if !server.checkOrigin("http://localhost:3000", "any-host") {
			t.Error("Should allow http://localhost:3000")
		}

		if server.checkOrigin("https://other.com", "any-host") {
			t.Error("Should not allow unlisted origin")
		}
	})

	t.Run("Regex patterns", func(t *testing.T) {
		server.SetOriginPatterns([]string{"^https://.*\\.example\\.com$"})

		if !server.checkOrigin("https://sub.example.com", "any-host") {
			t.Error("Should allow https://sub.example.com")
		}

		if !server.checkOrigin("https://deep.sub.example.com", "any-host") {
			t.Error("Should allow https://deep.sub.example.com")
		}

		if server.checkOrigin("http://sub.example.com", "any-host") {
			t.Error("Should not allow http (pattern requires https)")
		}

		if server.checkOrigin("https://example.com", "any-host") {
			t.Error("Should not allow root domain (pattern requires subdomain)")
		}
	})

	t.Run("Patterns configured - request host is still allowed", func(t *testing.T) {
		// A page and its socket share an origin; extra patterns widen the allow-list for
		// cross-origin callers and must not take same-origin connections away.
		server.SetOriginPatterns([]string{"admin.example.com"})

		if !server.checkOrigin("https://erp.client.com", "erp.client.com") {
			t.Error("Should allow origin matching request host even with patterns set")
		}

		if !server.checkOrigin("https://admin.example.com", "erp.client.com") {
			t.Error("Should still allow configured pattern")
		}

		if server.checkOrigin("https://evil.com", "erp.client.com") {
			t.Error("Should not allow origin that matches neither host nor patterns")
		}
	})

	t.Run("Trailing slash handling", func(t *testing.T) {
		server.SetOriginPatterns([]string{"example.com"})

		if !server.checkOrigin("https://example.com/", "any-host") {
			t.Error("Should allow origin with trailing slash")
		}
	})
}

func TestServerHandler(t *testing.T) {
	server := NewServer()
	server.SetLogger(NewNoopLogger())

	handler := server.Handler()

	if handler == nil {
		t.Error("Handler() should not return nil")
	}

	// Test that it implements http.Handler
	var _ http.Handler = handler
}

func TestServerServeHTTP(t *testing.T) {
	server := NewServer()
	server.SetLogger(NewNoopLogger())
	server.SetOriginPatterns([]string{"*"})

	// Create namespace
	ns := server.Of("/")

	t.Run("Wrong path", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/wrong-path", nil)
		w := httptest.NewRecorder()

		server.ServeHTTP(w, req)

		if w.Code != http.StatusNotFound {
			t.Errorf("Status code = %d, want %d", w.Code, http.StatusNotFound)
		}
	})

	t.Run("Namespace not found", func(t *testing.T) {
		req := httptest.NewRequest("GET", DefaultWSPath+"?namespace=/nonexistent", nil)
		w := httptest.NewRecorder()

		server.ServeHTTP(w, req)

		if w.Code != http.StatusNotFound {
			t.Errorf("Status code = %d, want %d", w.Code, http.StatusNotFound)
		}
	})

	t.Run("Origin not allowed", func(t *testing.T) {
		server2 := NewServer()
		server2.SetLogger(NewNoopLogger())
		server2.SetOriginPatterns([]string{"example.com"})
		server2.Of("/")

		req := httptest.NewRequest("GET", DefaultWSPath, nil)
		req.Header.Set("Origin", "http://malicious.com")
		w := httptest.NewRecorder()

		server2.ServeHTTP(w, req)

		if w.Code != http.StatusForbidden {
			t.Errorf("Status code = %d, want %d", w.Code, http.StatusForbidden)
		}
	})

	// Note: Cannot fully test WebSocket upgrade without actual WebSocket client
	// The websocket.Accept() will fail in tests, which is expected
	_ = ns // Silence unused warning
}

func TestServerClose(t *testing.T) {
	server := NewServer()
	server.SetLogger(NewNoopLogger())

	// Create some namespaces
	server.Of("/ns1")
	server.Of("/ns2")
	server.Of("/ns3")

	// Close server
	server.Close()

	// Namespaces should be nil
	server.mu.RLock()
	nsList := server.namespaces
	server.mu.RUnlock()

	if nsList != nil {
		t.Error("namespaces should be nil after Close()")
	}
}

func TestServerCloseWithAdapter(t *testing.T) {
	server := NewServer()
	server.SetLogger(NewNoopLogger())

	mockAdapter := &mockAdapter{}
	server.SetAdapter(mockAdapter)

	server.Close()

	if !mockAdapter.closed {
		t.Error("Close() should close adapter")
	}
}

// newLimitServer returns a server with a 1KB read limit whose "msg" events are sent to got
func newLimitServer() (*Server, chan string) {
	s := newTestServer()
	s.SetOriginPatterns([]string{"*"})
	s.SetPingInterval(0)
	s.SetReadLimit(1024)
	got := make(chan string, 1)
	s.Of("/").On("msg", func(_ *ConnContext, data any) { got <- data.(string) })
	return s, got
}

// assertReadLimit checks a message within the limit is handled and a larger one closes with 1009
func assertReadLimit(t *testing.T, url string, got chan string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	small := dialWS(t, url)
	if err := wsjson.Write(ctx, small, Message{Event: "msg", Data: "hi"}); err != nil {
		t.Fatalf("write: %v", err)
	}
	select {
	case v := <-got:
		if v != "hi" {
			t.Errorf("got %q, want hi", v)
		}
	case <-ctx.Done():
		t.Fatal("message within limit was not processed")
	}

	big := dialWS(t, url)
	if err := wsjson.Write(ctx, big, Message{Event: "msg", Data: strings.Repeat("x", 2048)}); err != nil {
		t.Fatalf("write: %v", err)
	}
	_, _, err := big.Read(ctx)
	if code := websocket.CloseStatus(err); code != websocket.StatusMessageTooBig {
		t.Errorf("close status = %v (err %v), want 1009", code, err)
	}
	select {
	case <-got:
		t.Error("message over limit must not be processed")
	default:
	}
}

func TestServerReadLimit(t *testing.T) {
	s, got := newLimitServer()
	srv := httptest.NewServer(s.Handler())
	defer srv.Close()
	assertReadLimit(t, "ws"+strings.TrimPrefix(srv.URL, "http")+DefaultWSPath, got)
}

// newCloseServer returns a server whose connections are closed with 4401 right after connecting
func newCloseServer() *Server {
	s := newTestServer()
	s.SetOriginPatterns([]string{"*"})
	s.SetPingInterval(0)
	s.Of("/").OnConnect(func(ctx *ConnContext) error {
		go ctx.CloseWith(4401, "unauthorized")
		return nil
	})
	return s
}

// assertCloseFrame checks the client receives close code 4401 with its reason
func assertCloseFrame(t *testing.T, url string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, _, err := dialWS(t, url).Read(ctx)
	var ce websocket.CloseError
	if !errors.As(err, &ce) || ce.Code != 4401 || ce.Reason != "unauthorized" {
		t.Errorf("close = %v, want 4401 unauthorized", err)
	}
}

func TestServerCloseWithCode(t *testing.T) {
	srv := httptest.NewServer(newCloseServer().Handler())
	defer srv.Close()
	assertCloseFrame(t, "ws"+strings.TrimPrefix(srv.URL, "http")+DefaultWSPath)
}

// newCloseInOnConnectServer closes from inside OnConnect, before the connection is
// registered: the close must still reach the client instead of being dropped.
func newCloseInOnConnectServer() *Server {
	s := newTestServer()
	s.SetOriginPatterns([]string{"*"})
	s.SetPingInterval(0)
	s.Of("/").OnConnect(func(ctx *ConnContext) error {
		ctx.CloseWith(4401, "unauthorized")
		return nil
	})
	return s
}

func TestServerCloseWithInsideOnConnect(t *testing.T) {
	srv := httptest.NewServer(newCloseInOnConnectServer().Handler())
	defer srv.Close()
	assertCloseFrame(t, "ws"+strings.TrimPrefix(srv.URL, "http")+DefaultWSPath)
}

// Mock adapter for testing
type mockAdapter struct {
	loggerSet bool
	closed    bool

	mu         sync.Mutex
	handler    func(data BroadcastData)
	broadcasts []BroadcastData
}

func (m *mockAdapter) Broadcast(namespace string, data BroadcastData) {
	m.mu.Lock()
	m.broadcasts = append(m.broadcasts, data)
	m.mu.Unlock()
}

func (m *mockAdapter) Subscribe(namespace string, handler func(data BroadcastData)) {
	m.mu.Lock()
	m.handler = handler
	m.mu.Unlock()
}

func (m *mockAdapter) Unsubscribe(namespace string) {}

func (m *mockAdapter) SetLogger(logger Logger) {
	m.loggerSet = true
}

func (m *mockAdapter) Close() {
	m.closed = true
}

// deliver simulates a payload arriving from another node
func (m *mockAdapter) deliver(data BroadcastData) {
	m.mu.Lock()
	h := m.handler
	m.mu.Unlock()
	h(data)
}

func (m *mockAdapter) sent() []BroadcastData {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]BroadcastData(nil), m.broadcasts...)
}

func BenchmarkServerOf(b *testing.B) {
	server := NewServer()
	server.SetLogger(NewNoopLogger())

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = server.Of("/test")
	}
}

func BenchmarkServerCheckOrigin(b *testing.B) {
	server := NewServer()
	server.SetOriginPatterns([]string{"example.com", "localhost:3000"})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = server.checkOrigin("https://example.com", "test.com")
	}
}
