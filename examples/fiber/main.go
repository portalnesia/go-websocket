package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/redis/go-redis/v9"
	ws "go.portalnesia.com/websocket"
	wsRedis "go.portalnesia.com/websocket/adapter/redis"
)

func main() {
	// Create server for Fiber
	server := ws.NewFiberServer()
	server.SetOriginPatterns([]string{"*"}) // Allow all origins for demo

	// Connection limits (these are the defaults)
	server.SetPingInterval(ws.DefaultPingInterval) // 0 disables heartbeat
	server.SetPongTimeout(ws.DefaultPongTimeout)
	server.SetWriteTimeout(ws.DefaultWriteTimeout)
	server.SetReadLimit(ws.DefaultReadLimit) // larger messages close the connection with 1009

	// Setup Redis adapter (any redis.UniversalClient: single node, cluster, sentinel)
	redisClient := redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs: []string{"localhost:6379"},
	})

	adapter, err := wsRedis.New(redisClient, &wsRedis.Config{
		MaxPublishGoros: 50,
		ErrorCallback: func(err error) {
			log.Printf("Redis error: %v", err)
		},
	})
	if err != nil {
		log.Fatalf("Failed to create Redis adapter: %v", err)
	}

	server.SetAdapter(adapter)

	// Create namespace
	ns := server.Of("/")

	ns.OnConnect(func(ctx *ws.ConnContext) error {
		token := ctx.Query("token")

		if token == "" {
			return fmt.Errorf("token required")
		}

		// Validate token (implement your own logic)
		if token != "valid-token" {
			return fmt.Errorf("invalid token")
		}

		// Store username in local state
		username := ctx.Query("username")
		if username == "" {
			username = "User_" + token
		}
		ctx.Local("username", username)

		log.Printf("Client connected: %s", username)
		return nil
	})

	ns.OnDisconnect(func(ctx *ws.ConnContext) {
		log.Printf("Client disconnected: %s", ctx.ID())
	})

	ns.On("chat:message", func(ctx *ws.ConnContext, data any) {
		log.Printf("Received chat message: %v", data)

		// Get username from local state
		username := ctx.Local("username")
		if username == nil {
			username = "Anonymous"
		}

		// Broadcast to all clients (including sender)
		ns.Emit(ws.Message{
			Event: "chat:message", Data: map[string]any{
				"username": username,
				"message":  data,
			},
		})
	})

	// Event: room:join
	ns.On("room:join", func(ctx *ws.ConnContext, data any) {
		roomName, ok := data.(string)
		if !ok {
			return
		}

		username := ctx.Local("username")
		if username == nil {
			username = "Anonymous"
		}

		// Join the room
		ctx.JoinRoom(roomName)
		log.Printf("%v joined room: %s", username, roomName)

		// Notify user they joined
		if err := ctx.Emit("room:joined", map[string]any{
			"room": roomName,
		}); err != nil {
			log.Printf("Failed to emit room:joined: %v", err)
		}

		// Notify others in room (except sender)
		ctx.BroadcastToRoom(roomName, ws.Message{
			Event: "room:user_joined", Data: map[string]any{
				"username": username,
				"room":     roomName,
			},
		})
	})

	// Event: room:leave
	ns.On("room:leave", func(ctx *ws.ConnContext, data any) {
		roomName, ok := data.(string)
		if !ok {
			return
		}

		username := ctx.Local("username")

		// Leave the room
		ctx.LeaveRoom(roomName)
		log.Printf("%v left room: %s", username, roomName)

		// Notify others in room
		ctx.BroadcastToRoom(roomName, ws.Message{
			Event: "room:user_left", Data: map[string]any{
				"username": username,
				"room":     roomName,
			},
		})
	})

	// Event: room:message
	ns.On("room:message", func(ctx *ws.ConnContext, data any) {
		dataMap, ok := data.(map[string]any)
		if !ok {
			return
		}

		roomName, ok := dataMap["room"].(string)
		if !ok {
			return
		}

		message := dataMap["message"]
		username := ctx.Local("username")

		// Broadcast to room only
		ns.EmitToRoom(roomName, ws.Message{
			Event: "room:message", Data: map[string]any{
				"username": username,
				"room":     roomName,
				"message":  message,
			},
		})

		log.Printf("[Room %s] %v: %v", roomName, username, message)
	})

	// Event: ping
	ns.On("ping", func(ctx *ws.ConnContext, data any) {
		log.Println("Received ping")

		// Send pong back to sender only
		if err := ctx.Emit("pong", map[string]any{
			"message": "pong",
		}); err != nil {
			log.Printf("Failed to emit pong: %v", err)
		}
	})

	// Create HTTP server
	httpServer := fiber.New()

	server.FiberHandler(httpServer)

	// Start server in goroutine
	go func() {
		log.Println("Server starting on :8080")
		if errFiber := httpServer.Listen(":8080"); errFiber != nil {
			log.Fatalf("Server error: %v", errFiber)
		}
	}()

	// Wait for interrupt signal
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down server...")

	// Graceful shutdown with timeout
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 1. Stop accepting new connections
	if err := httpServer.ShutdownWithContext(shutdownCtx); err != nil {
		log.Printf("HTTP server shutdown error: %v", err)
	}

	// 2. Close all websocket connections and cleanup namespaces
	// This will call Unsubscribe() for each namespace
	server.Close()

	// 3. Close Redis connection
	if err := redisClient.Close(); err != nil {
		log.Printf("Redis close error: %v", err)
	}

	log.Println("Server gracefully stopped")
}
