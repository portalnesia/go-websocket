package main

import (
	"fmt"
	"log"
	"os"

	"github.com/redis/go-redis/v9"
	ws "go.portalnesia.com/websocket"
	wsRedis "go.portalnesia.com/websocket/adapter/redis"
)

func main() {
	// Get node ID from environment or default
	nodeID := os.Getenv("NODE_ID")
	if nodeID == "" {
		nodeID = "node-1"
	}

	// Get Redis address from environment or default
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}

	// Create server
	server := ws.NewServer()

	redisConn := redis.NewClient(&redis.Options{
		Addr:     redisAddr,
		Password: "", // Set if Redis has password
		DB:       0,
	})
	defer redisConn.Close()

	// Create Redis adapter for multi-node support
	redisAdapter, err := wsRedis.New(redisConn)
	if err != nil {
		log.Fatalf("Failed to create Redis adapter: %v", err)
	}
	defer redisAdapter.Close()

	log.Printf("[%s] Redis adapter connected to %s", nodeID, redisAddr)
	server.SetAdapter(redisAdapter)

	// Get default namespace
	ns := server.Of("/")

	// Set Redis adapter for multi-node broadcasting

	log.Printf("[%s] Multi-node broadcasting enabled via Redis", nodeID)

	// OnConnect
	ns.OnConnect(func(ctx *ws.ConnContext) error {
		token := ctx.Query("token")
		if token == "" {
			return fmt.Errorf("token required")
		}

		if token != "valid-token" {
			return fmt.Errorf("invalid token")
		}

		username := ctx.Query("username")
		if username == "" {
			username = "User_" + token
		}

		ctx.Local("username", username)
		ctx.Local("nodeID", nodeID) // Store which node this connection is on

		log.Printf("[%s] Client connected: %s (connID: %s)", nodeID, username, ctx.ID())
		return nil
	})

	// OnDisconnect
	ns.OnDisconnect(func(ctx *ws.ConnContext) {
		username := ctx.Local("username")
		log.Printf("[%s] Client disconnected: %v", nodeID, username)
	})

	// Event: chat:message (broadcasts to ALL nodes)
	ns.On("chat:message", func(ctx *ws.ConnContext, data any) {
		username := ctx.Local("username")
		connNodeID := ctx.Local("nodeID")

		log.Printf("[%s] Received chat from %v: %v", nodeID, username, data)

		// Broadcast to ALL nodes via Redis
		ns.Emit(ws.Message{
			Event: "chat:message",
			Data: map[string]any{
				"username":  username,
				"message":   data,
				"from_node": connNodeID,
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
		ctx.JoinRoom(roomName)

		log.Printf("[%s] %v joined room: %s", nodeID, username, roomName)

		// Notify user
		if err := ctx.Emit("room:joined", map[string]any{
			"room": roomName,
		}); err != nil {
			log.Printf("[%s] Failed to emit room:joined: %v", nodeID, err)
		}

		// Notify others in room (ALL nodes)
		ns.EmitToRoom(roomName, ws.Message{
			Event: "room:user_joined",
			Data: map[string]any{
				"username": username,
				"room":     roomName,
				"node":     nodeID,
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
		ctx.LeaveRoom(roomName)

		log.Printf("[%s] %v left room: %s", nodeID, username, roomName)

		// Notify others in room (ALL nodes)
		ns.EmitToRoom(roomName, ws.Message{
			Event: "room:user_left", Data: map[string]any{
				"username": username,
				"room":     roomName,
			},
		})
	})

	// Event: room:message (broadcasts to ALL nodes in room)
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
		connNodeID := ctx.Local("nodeID")

		// Broadcast to room on ALL nodes via Redis
		ns.EmitToRoom(roomName, ws.Message{
			Event: "room:message", Data: map[string]any{
				"username":  username,
				"room":      roomName,
				"message":   message,
				"from_node": connNodeID,
			},
		})

		log.Printf("[%s] [Room %s] %v: %v", nodeID, roomName, username, message)
	})

	// Event: node:info (get info about this node)
	ns.On("node:info", func(ctx *ws.ConnContext, data any) {
		ctx.Emit("node:info", map[string]any{
			"nodeID":      nodeID,
			"connections": ns.ConnectionCount(),
		})
	})

	// Event: ping
	ns.On("ping", func(ctx *ws.ConnContext, data any) {
		log.Printf("[%s] Received ping", nodeID)

		if err := ctx.Emit("pong", map[string]any{
			"message": "pong",
			"node":    nodeID,
		}); err != nil {
			log.Printf("[%s] Failed to emit pong: %v", nodeID, err)
		}
	})

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	log.Printf("[%s] WebSocket server starting on :%s", nodeID, port)
	log.Printf("[%s] Connect to: ws://localhost:%s/pn-socket?token=valid-token&username=YourName", nodeID, port)
	log.Println("")
	log.Println("Multi-node features:")
	log.Println("- Chat messages broadcast to ALL nodes")
	log.Println("- Room messages broadcast to ALL nodes")
	log.Println("- Users can connect to any node and chat together")
	log.Println("")
	log.Printf("To run multiple nodes:")
	log.Printf("  NODE_ID=node-1 PORT=8080 go run main.go")
	log.Printf("  NODE_ID=node-2 PORT=8081 go run main.go")
	log.Printf("  NODE_ID=node-3 PORT=8082 go run main.go")

	if err := server.Listen(":" + port); err != nil {
		log.Fatal(err)
	}
}
