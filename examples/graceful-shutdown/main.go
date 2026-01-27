package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	ws "go.portalnesia.com/websocket"
	wsRedis "go.portalnesia.com/websocket/adapter/redis"
)

func main() {
	// Create server
	server := ws.NewServer()
	server.SetOriginPatterns([]string{"*"}) // Allow all origins for demo

	// Setup Redis adapter
	redisClient := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
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
		log.Printf("Client connected: %s", ctx.ID())
		return nil
	})

	ns.OnDisconnect(func(ctx *ws.ConnContext) {
		log.Printf("Client disconnected: %s", ctx.ID())
	})

	ns.On("message", func(ctx *ws.ConnContext, data any) {
		log.Printf("Message from %s: %v", ctx.ID(), data)
		ns.Emit(ws.Message{
			Event: "message",
			Data:  data,
		})
	})

	// Create HTTP server
	httpServer := &http.Server{
		Addr:    ":8080",
		Handler: server.Handler(),
	}

	// Start server in goroutine
	go func() {
		log.Println("Server starting on :8080")
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server error: %v", err)
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
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
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
