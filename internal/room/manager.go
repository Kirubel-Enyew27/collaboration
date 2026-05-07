package room

import (
	"context"
	"errors"
	"sync"

	"collaboration/internal/metrics"
	"collaboration/internal/store"

	"go.uber.org/zap"
)

// Participant represents the minimal behavior required by the room manager.
// Implemented by types in other packages (e.g. ws.Client) without creating
// an import cycle.
type Participant interface {
	GetID() string
	SendMessage([]byte)
}

// Manager manages multiple rooms and their participants.
type Manager struct {
	mu     sync.RWMutex
	rooms  map[string]*Room
	logger *zap.Logger
	repo   store.RoomRepository
}

// Room represents a collaboration room with participants keyed by participant ID.
type Room struct {
	name         string
	participants map[string]Participant
}

// NewManager creates a room manager.
func NewManager(logger *zap.Logger, repo store.RoomRepository) *Manager {
	return &Manager{
		rooms:  make(map[string]*Room),
		logger: logger,
		repo:   repo,
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
	m.rooms[name] = &Room{name: name, participants: make(map[string]Participant)}
	m.logger.Info("room created", zap.String("room", name))
	if m.repo != nil {
		// best-effort persist
		_ = m.repo.CreateRoom(context.Background(), name)
	}
	// metrics
	metrics.SetRooms(float64(len(m.rooms)))
	return nil
}

// Join adds a participant to the named room, creating the room if necessary.
func (m *Manager) Join(name string, p Participant) error {
	if name == "" {
		return errors.New("room name required")
	}
	m.mu.Lock()
	r, ok := m.rooms[name]
	if !ok {
		r = &Room{name: name, participants: make(map[string]Participant)}
		m.rooms[name] = r
		m.logger.Info("room auto-created on join", zap.String("room", name))
	}
	r.participants[p.GetID()] = p
	m.mu.Unlock()
	m.logger.Debug("participant joined room", zap.String("room", name), zap.String("participant", p.GetID()))
	// metrics
	metrics.SetParticipantsTotal(float64(func() int { m.mu.RLock(); defer m.mu.RUnlock(); count := 0; for _, rr := range m.rooms { count += len(rr.participants) }; return count }()))
	metrics.SetParticipantsPerRoom(name, float64(len(r.participants)))
	return nil
}

// Leave removes a participant from a room. If the room becomes empty it is deleted.
func (m *Manager) Leave(name string, p Participant) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.rooms[name]
	if !ok {
		return nil
	}
	delete(r.participants, p.GetID())
	m.logger.Debug("participant left room", zap.String("room", name), zap.String("participant", p.GetID()))
	if len(r.participants) == 0 {
		delete(m.rooms, name)
		m.logger.Info("room removed (empty)", zap.String("room", name))
		if m.repo != nil {
			// best-effort delete persisted room
			_ = m.repo.DeleteRoom(context.Background(), name)
		}
		metrics.SetRooms(float64(len(m.rooms)))
	}
	// update participants metrics
	metrics.SetParticipantsTotal(float64(func() int { m.mu.RLock(); defer m.mu.RUnlock(); count := 0; for _, rr := range m.rooms { count += len(rr.participants) }; return count }()))
	if r != nil {
		metrics.SetParticipantsPerRoom(name, float64(len(r.participants)))
	}
	return nil
}

// Broadcast sends a message to all participants in the room.
func (m *Manager) Broadcast(name string, message []byte) error {
	m.mu.RLock()
	r, ok := m.rooms[name]
	if !ok {
		m.mu.RUnlock()
		return errors.New("room not found")
	}
	// Copy participants slice under lock to avoid concurrent map iteration
	participants := make([]Participant, 0, len(r.participants))
	for _, p := range r.participants {
		participants = append(participants, p)
	}
	m.mu.RUnlock()

	for _, p := range participants {
		p.SendMessage(message)
	}
	metrics.IncBroadcast(name)
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
