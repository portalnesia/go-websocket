package websocket

import (
	"net/http"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
)

const (
	// DefaultWSPath is the default WebSocket endpoint path
	DefaultWSPath = "/pn-socket"

	// DefaultPingInterval is the default interval between heartbeat pings
	DefaultPingInterval = 25 * time.Second
	// DefaultPongTimeout is the default time to wait for a pong before closing the connection
	DefaultPongTimeout = 10 * time.Second
	// DefaultWriteTimeout is the default timeout for writing one message
	DefaultWriteTimeout = 10 * time.Second
	// DefaultReadLimit is the default max size in bytes of an incoming message
	DefaultReadLimit = 64 << 10
)

type Server struct {
	id              string
	mu              sync.RWMutex
	namespaces      map[string]*Namespace
	wsPath          string
	originPatterns  []string
	originRegexps   []*regexp.Regexp
	allowAllOrigins bool

	// Adapter for multi-node support (optional)
	adapter Adapter

	// Logger for logging (default: NewDefaultLogger())
	logger Logger

	pingInterval time.Duration
	pongTimeout  time.Duration
	writeTimeout time.Duration
	readLimit    int64
}

func NewServer() *Server {
	return &Server{
		id:              generateId(),
		namespaces:      make(map[string]*Namespace),
		wsPath:          DefaultWSPath,
		originPatterns:  []string{},
		allowAllOrigins: false,
		logger:          NewDefaultLogger(),
		pingInterval:    DefaultPingInterval,
		pongTimeout:     DefaultPongTimeout,
		writeTimeout:    DefaultWriteTimeout,
		readLimit:       DefaultReadLimit,
	}
}

// SetPingInterval sets the heartbeat ping interval (0 = heartbeat disabled).
// Call before serving connections.
func (s *Server) SetPingInterval(d time.Duration) {
	s.pingInterval = d
}

// SetPongTimeout sets how long to wait for a pong before closing the connection
func (s *Server) SetPongTimeout(d time.Duration) {
	s.pongTimeout = d
}

// SetWriteTimeout sets the timeout for writing one message (0 = no timeout).
// A write exceeding it closes the connection.
func (s *Server) SetWriteTimeout(d time.Duration) {
	s.writeTimeout = d
}

// SetReadLimit sets the max size in bytes of an incoming message (<= 0 = unlimited).
// A larger message closes the connection with status 1009.
func (s *Server) SetReadLimit(n int64) {
	s.readLimit = n
}

// readWindow is how long a connection may stay silent (no pong) before reads time out
func (s *Server) readWindow() time.Duration {
	if s.pingInterval <= 0 {
		return 0
	}
	return s.pingInterval + s.pongTimeout
}

// SetAdapter sets the adapter for multi-node support
func (s *Server) SetAdapter(adapter Adapter) {
	s.adapter = adapter
	// Propagate logger to adapter
	if s.adapter != nil {
		s.adapter.SetLogger(s.GetLogger())
	}
}

// SetLogger sets the logger for the server and all namespaces
func (s *Server) SetLogger(logger Logger) {
	if logger == nil {
		logger = NewNoopLogger()
	}
	s.logger = logger
	// Propagate to adapter if exists
	if s.adapter != nil {
		s.adapter.SetLogger(logger)
	}
}

// GetLogger returns the current logger, never returns nil
func (s *Server) GetLogger() Logger {
	if s.logger == nil {
		return NewNoopLogger()
	}
	return s.logger
}

// Close closes all namespaces and cleans up server resources
func (s *Server) Close() {
	s.GetLogger().Info("Closing server")
	s.mu.Lock()
	namespaces := make([]*Namespace, 0, len(s.namespaces))
	for _, ns := range s.namespaces {
		namespaces = append(namespaces, ns)
	}
	s.namespaces = nil
	s.mu.Unlock()

	// Close all namespaces
	for _, ns := range namespaces {
		ns.close()
	}

	// Close adapter
	if s.adapter != nil {
		s.adapter.Close()
	}
	s.GetLogger().Info("Server closed")
}

func (s *Server) Of(name string) *Namespace {
	// Check if namespace exists (read lock)
	s.mu.RLock()
	if ns, ok := s.namespaces[name]; ok {
		s.mu.RUnlock()
		return ns
	}
	s.mu.RUnlock()

	// Create new namespace (write lock)
	s.mu.Lock()
	defer s.mu.Unlock()

	// Double-check after acquiring write lock
	if ns, ok := s.namespaces[name]; ok {
		return ns
	}

	ns := &Namespace{
		name:   name,
		conns:  make(map[string]*connWrapper),
		server: s,
	}

	ns.init()

	s.namespaces[name] = ns
	s.GetLogger().Debug("Namespace created", "namespace", name)
	return ns
}

// SetWSPath sets custom WebSocket path
func (s *Server) SetWSPath(path string) {
	// always start with /
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	s.wsPath = path
}

// SetOriginPatterns sets allowed origin patterns
// Origin validation behavior:
//   - Always: allows origin matching the request host
//     (e.g., if running on https://domain.com, https://domain.com is allowed)
//   - To allow additional origins:
//     server.SetOriginPatterns([]string{"example.com", "localhost:3000"})
//   - To allow all origins (NOT recommended for production):
//     server.SetOriginPatterns([]string{"*"})
//   - To use regex patterns:
//     server.SetOriginPatterns([]string{"^https?://.*\\.myapp\\.com$"})
func (s *Server) SetOriginPatterns(patterns []string) {
	s.originPatterns = patterns
	s.originRegexps = make([]*regexp.Regexp, 0)
	s.allowAllOrigins = false

	for _, pattern := range patterns {
		if pattern == "*" {
			s.allowAllOrigins = true
			return
		}

		// Compile regex patterns (starts with ^, ends with $, or contains .* or \.)
		if strings.HasPrefix(pattern, "^") || strings.HasSuffix(pattern, "$") ||
			strings.Contains(pattern, ".*") || strings.Contains(pattern, "\\.") {
			if re, err := regexp.Compile(pattern); err == nil {
				s.originRegexps = append(s.originRegexps, re)
			}
		}
	}
}

// checkOrigin validates origin against configured patterns
func (s *Server) checkOrigin(origin string, host string) bool {
	if s.allowAllOrigins {
		return true
	}

	if origin == "" {
		return false
	}

	// Normalize once
	normalizedOrigin := strings.TrimSuffix(origin, "/")

	// The request host is always an allowed origin: a page and its socket share an origin,
	// and configured patterns only add cross-origin callers on top of it.
	if normalizedOrigin == "https://"+host || normalizedOrigin == "http://"+host {
		return true
	}
	if len(s.originPatterns) == 0 && len(s.originRegexps) == 0 {
		return false
	}

	// Check regex patterns first (usually more specific)
	for _, re := range s.originRegexps {
		if re.MatchString(normalizedOrigin) {
			return true
		}
	}

	// Check exact patterns
	for _, pattern := range s.originPatterns {
		// Skip patterns already in regex list
		if pattern == "*" || strings.HasPrefix(pattern, "^") || strings.Contains(pattern, ".*") {
			continue
		}

		normalizedPattern := pattern
		// Add https:// if no scheme
		if !strings.Contains(pattern, "://") {
			normalizedPattern = "https://" + pattern
		}
		normalizedPattern = strings.TrimSuffix(normalizedPattern, "/")

		if normalizedOrigin == normalizedPattern {
			return true
		}

		// Try http if https didn't match
		if strings.HasPrefix(normalizedPattern, "https://") {
			httpPattern := strings.Replace(normalizedPattern, "https://", "http://", 1)
			if normalizedOrigin == httpPattern {
				return true
			}
		}
	}

	return false
}

// Handler returns the HTTP handler for WebSocket connections
// Use this to integrate with existing HTTP servers
func (s *Server) Handler() http.Handler {
	return http.HandlerFunc(s.ServeHTTP)
}

// ServeHTTP implements http.Handler interface
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Only handle WebSocket path
	if r.URL.Path != s.wsPath {
		http.NotFound(w, r)
		return
	}

	// Validate origin
	if !s.allowAllOrigins {
		origin := r.Header.Get("Origin")
		host := r.Host
		if !s.checkOrigin(origin, host) {
			s.GetLogger().Debug("Origin not allowed", "origin", origin, "host", host)
			http.Error(w, "origin not allowed", http.StatusForbidden)
			return
		}
	}

	// Get namespace from query param or default "/"
	nsName := r.URL.Query().Get("namespace")
	if nsName == "" {
		nsName = "/"
	}

	s.mu.RLock()
	ns, ok := s.namespaces[nsName]
	s.mu.RUnlock()

	if !ok {
		s.GetLogger().Debug("Namespace not found", "namespace", nsName)
		http.Error(w, "namespace not found", http.StatusNotFound)
		return
	}

	ws, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		OriginPatterns: []string{"*"}, // Already validated above
	})
	if err != nil {
		s.GetLogger().Error("WebSocket accept failed", "error", err.Error())
		return
	}

	connID := generateId()
	ns.addConn(connID, newCoderWSConn(ws, s.writeTimeout), r)
}

// Listen starts the WebSocket server on the specified address
// Creates a new HTTP server internally
func (s *Server) Listen(addr string) error {
	s.GetLogger().Info("Starting WebSocket server", "address", addr, "wsPath", s.wsPath)
	err := http.ListenAndServe(addr, s.Handler())
	if err != nil {
		s.GetLogger().Error("Server listen failed", "error", err.Error())
	}
	return err
}
