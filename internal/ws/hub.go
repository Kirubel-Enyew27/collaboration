package ws

import (
	"sync"

	"go.uber.org/zap"
)

type Hub struct {
	clients    map[*Client]struct{}
	register   chan *Client
	unregister chan *Client
	mu         sync.RWMutex
	logger     *zap.Logger
	done       chan struct{}
}

func NewHub(logger *zap.Logger) *Hub {
	return &Hub{
		clients:    make(map[*Client]struct{}),
		register:   make(chan *Client),
		unregister: make(chan *Client),
		logger:     logger,
		done:       make(chan struct{}),
	}
}

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
			delete(h.clients, c)
			h.mu.Unlock()
			h.logger.Debug("client unregistered", zap.Int("client_count", h.Len()))
		case <-h.done:
			h.logger.Info("hub shutting down")
			return
		}
	}
}

func (h *Hub) Register(c *Client) {
	select {
	case h.register <- c:
	default:
		h.logger.Warn("register channel full, dropping client")
	}
}

func (h *Hub) Unregister(c *Client) {
	select {
	case h.unregister <- c:
	default:
		h.logger.Warn("unregister channel full")
	}
}

func (h *Hub) Len() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.clients)
}

func (h *Hub) Shutdown() {
	close(h.done)

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

func (h *Hub) RemoveClientByID(id string) *Client {
	h.mu.Lock()
	defer h.mu.Unlock()
	for c := range h.clients {
		if c.ID == id {
			delete(h.clients, c)
			return c
		}
	}
	return nil
}
