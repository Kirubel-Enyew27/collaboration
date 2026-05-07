package ws

import (
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
            h.logger.Debug("client registered", zap.Int("client_count", h.Len()))
        case c := <-h.unregister:
            h.mu.Lock()
            if _, ok := h.clients[c]; ok {
                delete(h.clients, c)
            }
            h.mu.Unlock()
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
