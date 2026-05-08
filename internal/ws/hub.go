package ws

import (
	"collaboration/internal/metrics"
	"sync"

	"go.uber.org/zap"
)

// Hub maintains the set of active clients and broadcasts messages to the
// clients. It's safe for concurrent use.
type Hub struct {
    clients    map[*Client]struct{}
    register   chan *Client
    unregister chan *Client
    mu         sync.RWMutex
    logger     *zap.Logger
    done       chan struct{}
}

// NewHub creates a new Hub instance.
func NewHub(logger *zap.Logger) *Hub {
    return &Hub{
        clients:    make(map[*Client]struct{}),
        register:   make(chan *Client),
        unregister: make(chan *Client),
        logger:     logger,
        done:       make(chan struct{}),
    }
}

// Run starts the main run loop for the hub processing registrations.
func (h *Hub) Run() {
    for {
        select {
        case c := <-h.register:
            h.mu.Lock()
            h.clients[c] = struct{}{}
            h.mu.Unlock()
            // metrics
            metrics.IncConnection()
            h.logger.Debug("client registered", zap.Int("client_count", h.Len()))
        case c := <-h.unregister:
            h.mu.Lock()
            delete(h.clients, c)
            h.mu.Unlock()
            metrics.DecConnection()
            h.logger.Debug("client unregistered", zap.Int("client_count", h.Len()))
        case <-h.done:
            h.logger.Info("hub shutting down")
            return
        }
    }
}

// Register adds a client to the hub.
func (h *Hub) Register(c *Client) {
    select {
    case h.register <- c:
    default:
        // Avoid blocking; log and drop
        h.logger.Warn("register channel full, dropping client")
    }
}

// Unregister removes a client from the hub.
func (h *Hub) Unregister(c *Client) {
    select {
    case h.unregister <- c:
    default:
        h.logger.Warn("unregister channel full")
    }
}

// Len returns the number of connected clients.
func (h *Hub) Len() int {
    h.mu.RLock()
    defer h.mu.RUnlock()
    return len(h.clients)
}

// GetClientByID returns a connected client with the given id, or nil.
func (h *Hub) GetClientByID(id string) *Client {
    h.mu.RLock()
    defer h.mu.RUnlock()
    for c := range h.clients {
        if c.ID == id {
            return c
        }
    }
    return nil
}

// RemoveClientByID removes the client with the given id from the hub and
// returns it for further cleanup. Caller should call Close() on the client.
func (h *Hub) RemoveClientByID(id string) *Client {
    h.mu.Lock()
    defer h.mu.Unlock()
    for c := range h.clients {
        if c.ID == id {
            delete(h.clients, c)
            // update metrics for immediate removal
            metrics.DecConnection()
            return c
        }
    }
    return nil
}

// Shutdown closes all client connections and stops the hub.
func (h *Hub) Shutdown() {
    // Signal run loop to exit
    close(h.done)

    // Close all clients
    h.mu.RLock()
    clients := make([]*Client, 0, len(h.clients))
    for c := range h.clients {
        clients = append(clients, c)
    }
    h.mu.RUnlock()

    for _, c := range clients {
        c.Close()
    }
}
