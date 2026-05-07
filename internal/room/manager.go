package room

import (
	"errors"
	"sync"

	"collaboration/internal/ws"

	"go.uber.org/zap"
)

// Manager manages multiple rooms and their participants.
type Manager struct {
	mu     sync.RWMutex
	rooms  map[string]*Room
	logger *zap.Logger
}

// Room represents a collaboration room with participants keyed by client ID.
type Room struct {
	name         string
	participants map[string]*ws.Client
}

// NewManager creates a room manager.
func NewManager(logger *zap.Logger) *Manager {
	return &Manager{
		rooms:  make(map[string]*Room),
		logger: logger,
	}
}

// CreateRoom creates a room if it doesn't already exist.
func (m *Manager) CreateRoom(name string) error {
	if name == "" {
		return errors.New("room name required")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.rooms[name]; ok {
		return nil
	}
	m.rooms[name] = &Room{name: name, participants: make(map[string]*ws.Client)}
	m.logger.Info("room created", zap.String("room", name))
	return nil
}

// Join adds a client to the named room, creating the room if necessary.
func (m *Manager) Join(name string, client *ws.Client) error {
	if name == "" {
		return errors.New("room name required")
	}
	m.mu.Lock()
	r, ok := m.rooms[name]
	if !ok {
		r = &Room{name: name, participants: make(map[string]*ws.Client)}
		m.rooms[name] = r
		m.logger.Info("room auto-created on join", zap.String("room", name))
	}
	r.participants[client.ID] = client
	m.mu.Unlock()

	// set client's room and onClose callback to ensure cleanup
	client.Room = name
	prev := client.OnClose
	client.OnClose = func(c *ws.Client) {
		m.Leave(name, c)
		if prev != nil {
			prev(c)
		}
	}

	m.logger.Debug("client joined room", zap.String("room", name), zap.String("client", client.ID))
	return nil
}

// Leave removes a client from a room. If the room becomes empty it is deleted.
func (m *Manager) Leave(name string, client *ws.Client) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.rooms[name]
	if !ok {
		return nil
	}
	delete(r.participants, client.ID)
	m.logger.Debug("client left room", zap.String("room", name), zap.String("client", client.ID))
	if len(r.participants) == 0 {
		delete(m.rooms, name)
		m.logger.Info("room removed (empty)", zap.String("room", name))
	}
	return nil
}

// Broadcast sends a message to all participants in the room.
func (m *Manager) Broadcast(name string, message []byte) error {
	m.mu.RLock()
	r, ok := m.rooms[name]
	m.mu.RUnlock()
	if !ok {
		return errors.New("room not found")
	}

	for _, p := range r.participants {
		p.SendMessage(message)
	}
	return nil
}

// Participants returns a slice of participant IDs for a room.
func (m *Manager) Participants(name string) ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	r, ok := m.rooms[name]
	if !ok {
		return nil, errors.New("room not found")
	}
	ids := make([]string, 0, len(r.participants))
	for id := range r.participants {
		ids = append(ids, id)
	}
	return ids, nil
}
