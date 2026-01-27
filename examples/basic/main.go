package main

import (
	"fmt"
	"log"

	ws "go.portalnesia.com/websocket"
)

func main() {
	server := ws.NewServer()

	// Optional: Set custom WebSocket path
	// server.SetWSPath("/ws")

	// Origin validation behavior:
	// - Default: Only allows origin matching the request host
	//   (e.g., if running on https://domain.com, only https://domain.com is allowed)
	// - To allow specific origins:
	//   server.SetOriginPatterns([]string{"example.com", "localhost:3000"})
	// - To allow all origins (NOT recommended for production):
	//   server.SetOriginPatterns([]string{"*"})
	// - To use regex patterns:
	//   server.SetOriginPatterns([]string{"^https?://.*\\.myapp\\.com$"})

	// Get default namespace
	ns := server.Of("/")

	// OnConnect: Authentication & validation
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

	// OnDisconnect: Cleanup
	ns.OnDisconnect(func(ctx *ws.ConnContext) {
		log.Println("Client disconnected")
	})

	// Event: chat:message
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
		ns.EmitToRoom(roomName, ws.Message{
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
		ns.EmitToRoom(roomName, ws.Message{
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

	log.Println("WebSocket server starting on :8080")
	log.Println("Connect to: ws://localhost:8080/pn-socket?token=valid-token")

	if err := server.Listen(":8080"); err != nil {
		log.Fatal(err)
	}
}
