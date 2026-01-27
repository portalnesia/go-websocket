# Multi-Node WebSocket Example with Redis

This example demonstrates how to run multiple WebSocket servers that communicate via Redis Pub/Sub.

## Architecture

```
Client 1 ──► Node 1 ──┐
Client 2 ──► Node 1 ──┤
                      │
Client 3 ──► Node 2 ──┼──► Redis Pub/Sub ──► All Nodes
Client 4 ──► Node 2 ──┤
                      │
Client 5 ──► Node 3 ──┘
Client 6 ──► Node 3 ──┘
```

When a client sends a message to any node, Redis broadcasts it to all nodes, and each node sends it to their local connections.

## Prerequisites

### 1. Install Redis

**macOS:**
```bash
brew install redis
brew services start redis
```

**Linux:**
```bash
sudo apt-get install redis-server
sudo systemctl start redis
```

**Windows:**
Download from: https://redis.io/download or use Docker

**Docker:**
```bash
docker run -d -p 6379:6379 redis:alpine
```

### 2. Verify Redis is Running

```bash
redis-cli ping
# Should return: PONG
```

## Running the Example

### Terminal 1 - Node 1:
```bash
cd examples/multinode
NODE_ID=node-1 PORT=8080 go run main.go
```

### Terminal 2 - Node 2:
```bash
cd examples/multinode
NODE_ID=node-2 PORT=8081 go run main.go
```

### Terminal 3 - Node 3:
```bash
cd examples/multinode
NODE_ID=node-3 PORT=8082 go run main.go
```

## Testing Multi-Node

### 1. Connect clients to different nodes:

**Client A → Node 1:**
```
ws://localhost:8080/pn-socket?token=valid-token&username=Alice
```

**Client B → Node 2:**
```
ws://localhost:8081/pn-socket?token=valid-token&username=Bob
```

**Client C → Node 3:**
```
ws://localhost:8082/pn-socket?token=valid-token&username=Charlie
```

### 2. Test Broadcasting:

**Client A sends chat message:**
```json
{
  "event": "chat:message",
  "data": "Hello from Node 1!"
}
```

**Result:** All clients (A, B, C) receive the message, even though they're on different nodes!

### 3. Test Rooms:

**All clients join same room:**
```json
{
  "event": "room:join",
  "data": "general"
}
```

**Client B sends room message:**
```json
{
  "event": "room:message",
  "data": {
    "room": "general",
    "message": "Hello room from Node 2!"
  }
}
```

**Result:** All clients in "general" room receive the message across all nodes!

## Configuration

### Environment Variables:

- `NODE_ID` - Unique identifier for this node (default: "node-1")
- `PORT` - Port to listen on (default: "8080")
- `REDIS_ADDR` - Redis server address (default: "localhost:6379")

### Example with Custom Redis:

```bash
NODE_ID=node-1 PORT=8080 REDIS_ADDR=redis.example.com:6379 go run main.go
```

## How It Works

### 1. **Namespace Broadcasting:**

```go
// Local broadcast + Redis publish
ns.Emit("chat:message", data)

// This does:
// 1. Publish to Redis channel: "pnws:ns:/"
// 2. Redis broadcasts to all subscribed nodes
// 3. Each node receives and broadcasts to local connections
```

### 2. **Room Broadcasting:**

```go
// Local room broadcast + Redis publish
ns.EmitToRoom("general", "room:message", data)

// This does:
// 1. Publish to Redis channel: "pnws:room:/general"
// 2. Redis broadcasts to all nodes with that room
// 3. Each node broadcasts to local connections in that room
```

### 3. **Auto-Subscription:**

When first connection joins a room:
```go
ctx.JoinRoom("general")

// Automatically subscribes this node to Redis channel:
// "pnws:room:/:general"
```

## Redis Channels

The adapter uses these Redis channels:

- **Namespace:** `pnws:ns:{namespace}`
  - Example: `pnws:ns:/`
  - Example: `pnws:ns:/chat`

## Monitoring Redis

### View active channels:
```bash
redis-cli PUBSUB CHANNELS pnws:*
```

### Monitor messages:
```bash
redis-cli --csv PSUBSCRIBE 'pnws:*'
```

### Count subscribers:
```bash
redis-cli PUBSUB NUMSUB pnws:ns:/
```

## Load Balancing

Use a load balancer (nginx, HAProxy) to distribute clients:

```nginx
upstream websocket {
    server localhost:8080;
    server localhost:8081;
    server localhost:8082;
}

server {
    location /pn-socket {
        proxy_pass http://websocket;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "Upgrade";
    }
}
```

## Performance Tips

1. **Redis Connection Pooling** - Already handled by go-redis
2. **Message Size** - Keep messages small for faster pub/sub
3. **Room Cleanup** - Rooms auto-unsubscribe when empty (TODO)
4. **Monitoring** - Use Redis monitoring for debugging

## Troubleshooting

### Clients not receiving messages across nodes:

1. Check Redis is running: `redis-cli ping`
2. Check Redis channels: `redis-cli PUBSUB CHANNELS pnws:*`
3. Check node logs for subscription confirmations
4. Verify all nodes connect to same Redis instance

### High Redis CPU:

- Reduce message frequency
- Use smaller message payloads
- Consider message batching

### Memory Growth:

- Check for room subscription leaks
- Monitor Redis memory: `redis-cli INFO memory`
- Set Redis maxmemory policy

## Next Steps

- Add sticky sessions for better performance
- Implement presence across nodes
- Add metrics and monitoring
- Add Redis cluster support for HA
