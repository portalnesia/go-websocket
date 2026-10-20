package websocket

import (
	"net/http"
	"net/url"

	"github.com/gofiber/fiber/v2"
	fiberws "github.com/gofiber/websocket/v2"
)

// NewFiberServer creates a new WebSocket server for Fiber framework
func NewFiberServer() *Server {
	return NewServer()
}

// FiberHandler integrates the WebSocket server with Fiber router
func (s *Server) FiberHandler(router fiber.Router) {
	router.Use(s.wsPath, func(c *fiber.Ctx) error {
		// Check if this is a WebSocket upgrade request
		if fiberws.IsWebSocketUpgrade(c) {
			// Store request info in locals for use in WebSocket handler
			c.Locals("wsPath", c.Path())
			c.Locals("wsQuery", string(c.Request().URI().QueryString()))
			c.Locals("wsHost", c.Hostname())
			c.Locals("wsHeaders", c.GetReqHeaders())
			return c.Next()
		}
		return fiber.ErrUpgradeRequired
	})

	router.Get(s.wsPath, fiberws.New(func(c *fiberws.Conn) {
		// Get stored values from locals
		path := c.Locals("wsPath").(string)
		queryString := c.Locals("wsQuery").(string)
		host := c.Locals("wsHost").(string)
		headers := c.Locals("wsHeaders").(map[string][]string)

		// Validate origin
		if !s.allowAllOrigins {
			origin := ""
			if originHeaders, ok := headers["Origin"]; ok && len(originHeaders) > 0 {
				origin = originHeaders[0]
			}
			if !s.checkOrigin(origin, host) {
				s.GetLogger().Debug("Origin not allowed", "origin", origin, "host", host)
				c.Close()
				return
			}
		}

		// Get namespace from query param or default "/"
		nsName := c.Query("namespace", "/")

		s.mu.RLock()
		ns, ok := s.namespaces[nsName]
		s.mu.RUnlock()

		if !ok {
			s.GetLogger().Debug("Namespace not found", "namespace", nsName)
			c.Close()
			return
		}

		// Build http.Request for context
		req := &http.Request{
			Method: "GET",
			URL: &url.URL{
				Path:     path,
				RawQuery: queryString,
			},
			Proto:      "HTTP/1.1",
			ProtoMajor: 1,
			ProtoMinor: 1,
			Header:     make(http.Header),
			Host:       host,
		}

		// Copy headers
		for key, values := range headers {
			for _, value := range values {
				req.Header.Add(key, value)
			}
		}

		// Generate connection ID and add to namespace
		connID := generateId()
		ctx := ns.addConn(connID, newFiberWSConn(c, s.writeTimeout, s.readWindow()), req)

		// Block until the connection is closed and every per-connection goroutine has
		// returned: gofiber/websocket recycles *Conn as soon as this handler returns, so a
		// read loop still inside ReadJSON would touch a released connection.
		if ctx != nil {
			<-ctx.stopped
		}
	}))
}
