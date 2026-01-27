package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	websocket "go.portalnesia.com/websocket"
)

// ErrorCallback is called when an error occurs
type ErrorCallback func(err error)

// Adapter implements multi-node broadcasting via Redis Pub/Sub
type Adapter struct {
	client *redis.Client
	ctx    context.Context
	cancel context.CancelFunc

	// Subscriptions and handlers
	subMu         sync.RWMutex
	subscriptions map[string]*subscription // namespace -> subscription

	// Publish semaphore to limit concurrent goroutines
	publishSem chan struct{}

	// Error callback (optional)
	errorCallback ErrorCallback

	// Logger
	logger websocket.Logger

	// WaitGroup to track goroutines
	wg sync.WaitGroup
}

// subscription holds pubsub and handler together
type subscription struct {
	pubsub  *redis.PubSub
	handler func(msg websocket.BroadcastData)
	cancel  context.CancelFunc
}

// Config for Redis adapter
type Config struct {
	Addr            string        // Redis address (default: "localhost:6379")
	Password        string        // Redis password (optional)
	DB              int           // Redis database (default: 0)
	MaxPublishGoros int           // Max concurrent publish goroutines (default: 100)
	PublishTimeout  time.Duration // Timeout for publish operations (default: 5s)
	ErrorCallback   ErrorCallback // Error callback (optional)
}

// New creates a new Redis adapter with optional config
func New(client *redis.Client, cfg ...*Config) (*Adapter, error) {
	// Test connection
	ctx := context.Background()
	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis connection failed: %w", err)
	}

	adapterCtx, cancel := context.WithCancel(ctx)

	// Default config
	maxPublishGoros := 100
	var errorCallback ErrorCallback

	if len(cfg) > 0 && cfg[0] != nil {
		if cfg[0].MaxPublishGoros > 0 {
			maxPublishGoros = cfg[0].MaxPublishGoros
		}
		errorCallback = cfg[0].ErrorCallback
	}

	return &Adapter{
		client:        client,
		ctx:           adapterCtx,
		cancel:        cancel,
		subscriptions: make(map[string]*subscription),
		publishSem:    make(chan struct{}, maxPublishGoros),
		errorCallback: errorCallback,
		logger:        websocket.NewNoopLogger(),
	}, nil
}

// SetLogger sets the logger for the adapter
func (r *Adapter) SetLogger(logger websocket.Logger) {
	if logger == nil {
		logger = websocket.NewNoopLogger()
	}
	r.logger = logger
}

// GetLogger returns the current logger (never nil)
func (r *Adapter) GetLogger() websocket.Logger {
	if r.logger == nil {
		r.logger = websocket.NewNoopLogger()
	}
	return r.logger
}

// publish is a DRY helper to publish messages to Redis with bounded concurrency
func (r *Adapter) publish(channel string, data any) {
	// Acquire semaphore
	select {
	case r.publishSem <- struct{}{}:
		r.wg.Add(1)
		go func() {
			defer func() {
				<-r.publishSem // Release semaphore
				r.wg.Done()
			}()

			jsonData, err := json.Marshal(data)
			if err != nil {
				r.handleError(fmt.Errorf("failed to marshal message: %w", err))
				return
			}

			// Use context with timeout
			ctx, cancel := context.WithTimeout(r.ctx, 5*time.Second)
			defer cancel()

			if err := r.client.Publish(ctx, channel, jsonData).Err(); err != nil {
				r.handleError(fmt.Errorf("failed to publish to %s: %w", channel, err))
			} else {
				r.GetLogger().Debug("Published to Redis",
					"channel", channel,
					"size", len(jsonData))
			}
		}()
	case <-r.ctx.Done():
		r.GetLogger().Warn("Publish skipped - context cancelled",
			"channel", channel)
		return
	}
}

// handleError logs error and calls error callback if set
func (r *Adapter) handleError(err error) {
	r.GetLogger().Error("Redis error", "error", err.Error())
	if r.errorCallback != nil {
		r.errorCallback(err)
	}
}

// Broadcast sends a message to all connections in a namespace across all nodes
func (r *Adapter) Broadcast(namespace string, data websocket.BroadcastData) {
	r.publish(r.makeNamespaceChannel(namespace), data)
}

// Subscribe subscribes to messages for a namespace
func (r *Adapter) Subscribe(namespace string, handler func(msg websocket.BroadcastData)) {
	channel := r.makeNamespaceChannel(namespace)

	r.subMu.Lock()
	defer r.subMu.Unlock()

	// Check if already subscribed
	if _, exists := r.subscriptions[namespace]; exists {
		// Update handler
		r.subscriptions[namespace].handler = handler
		return
	}

	// Create subscription context
	subCtx, subCancel := context.WithCancel(r.ctx)
	pubsub := r.client.Subscribe(subCtx, channel)

	sub := &subscription{
		pubsub:  pubsub,
		handler: handler,
		cancel:  subCancel,
	}

	r.subscriptions[namespace] = sub

	// Start listening in goroutine
	r.wg.Add(1)
	go r.listenNamespace(sub, namespace)

	r.GetLogger().Debug("Redis subscribed to namespace",
		"namespace", namespace,
		"channel", channel)
}

// Unsubscribe unsubscribes from a namespace
func (r *Adapter) Unsubscribe(namespace string) {
	r.subMu.Lock()
	sub, exists := r.subscriptions[namespace]
	if !exists {
		r.subMu.Unlock()
		return
	}
	delete(r.subscriptions, namespace)
	r.subMu.Unlock()

	// Cancel subscription context
	sub.cancel()

	// Close pubsub
	_ = sub.pubsub.Close()

	r.GetLogger().Debug("Redis unsubscribed from namespace",
		"namespace", namespace)
}

// listenNamespace listens for namespace broadcast messages from Redis
func (r *Adapter) listenNamespace(sub *subscription, namespace string) {
	defer r.wg.Done()

	ch := sub.pubsub.Channel()

	for {
		select {
		case <-r.ctx.Done():
			return
		case redisMsg, ok := <-ch:
			if !ok {
				return
			}

			var data websocket.BroadcastData
			if err := json.Unmarshal([]byte(redisMsg.Payload), &data); err != nil {
				r.handleError(fmt.Errorf("failed to unmarshal namespace message: %w", err))
				continue
			}

			// Call handler directly - no need for goroutine as we're already in one
			// Handler should be fast or spawn its own goroutine if needed
			if sub.handler != nil {
				sub.handler(data)
			}
		}
	}
}

// Close closes the adapter and cleans up resources
func (r *Adapter) Close() {
	r.GetLogger().Debug("Closing Redis adapter")

	// Cancel context to stop all operations
	r.cancel()

	// Close all subscriptions
	r.subMu.Lock()
	for _, sub := range r.subscriptions {
		sub.cancel()
		_ = sub.pubsub.Close()
	}
	r.subscriptions = nil // Clear map to prevent memory leak
	r.subMu.Unlock()

	// Wait for all goroutines to finish (with timeout)
	done := make(chan struct{})
	go func() {
		r.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// All goroutines finished
		r.GetLogger().Debug("Redis adapter closed successfully")
	case <-time.After(5 * time.Second):
		// Timeout waiting for goroutines
		r.GetLogger().Warn("Redis adapter close timeout waiting for goroutines")
	}
}

// Helper methods

func (r *Adapter) makeNamespaceChannel(namespace string) string {
	return fmt.Sprintf("pnws:ns:%s", namespace)
}
