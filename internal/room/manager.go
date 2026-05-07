package room

import (
	"errors"
	"sync"

	"go.uber.org/zap"
)

type Participant interface {
	GetID() string
	SendMessage([]byte)
}

type Manager struct {
	mu     sync.RWMutex
	rooms  map[string]*Room
	logger *zap.Logger
}

type Room struct {
	name         string
	participants map[string]Participant
}

func NewManager(logger *zap.Logger) *Manager {
	return &Manager{
		rooms:  make(map[string]*Room),
		logger: logger,
	}
}

func (m *Manager) CreateRoom(name string) error {
	if name == "" {
		return errors.New("room name required")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.rooms[name]; ok {
		return nil
	}
	m.rooms[name] = &Room{
		name:         name,
		participants: make(map[string]Participant),
	}
	m.logger.Info("room created", zap.String("room", name))
	return nil
}

func (m *Manager) Join(name string, p Participant) error {
	if name == "" {
		return errors.New("room name required")
	}
	m.mu.Lock()
	r, ok := m.rooms[name]
	if !ok {
		r = &Room{
			name:         name,
			participants: make(map[string]Participant),
		}
		m.rooms[name] = r
		m.logger.Info("room auto-created on join", zap.String("room", name))
	}
	r.participants[p.GetID()] = p
	m.mu.Unlock()

	m.logger.Debug("participant joined room", zap.String("room", name), zap.String("participant", p.GetID()))
	return nil
}

func (m *Manager) Leave(name string, p Participant) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.rooms[name]
	if !ok {
		return nil
	}
	delete(r.participants, p.GetID())
	m.logger.Debug("participant left room", zap.String("room", name),
		zap.String("participant", p.GetID()))

	if len(r.participants) == 0 {
		delete(m.rooms, name)
		m.logger.Info("room removed", zap.String("room", name))
	}
	return nil
}

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
