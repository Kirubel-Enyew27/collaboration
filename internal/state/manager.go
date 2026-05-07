package state

import (
	"collaboration/internal/room"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"

	"go.uber.org/zap"
)

type RoomState struct {
	mu      sync.RWMutex
	version int64
	data    json.RawMessage
}

type Manager struct {
	mu     sync.RWMutex
	rooms  map[string]*RoomState
	rm     *room.Manager
	logger *zap.Logger
}

func NewManager(rm *room.Manager, logger *zap.Logger) *Manager {
	return &Manager{rooms: make(map[string]*RoomState), rm: rm, logger: logger}
}

func (m *Manager) ensureRoom(name string) *RoomState {
	m.mu.Lock()
	r := m.rooms[name]
	m.mu.RUnlock()
	if r != nil {
		return r
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if r = m.rooms[name]; r == nil {
		r = &RoomState{version: 0, data: json.RawMessage(`{}`)}
		m.rooms[name] = r
	}
	return r
}

func (m *Manager) GetState(name string) (int64, json.RawMessage, error) {
	m.mu.RLock()
	r := m.rooms[name]
	m.mu.RUnlock()
	if r == nil {
		return 0, nil, errors.New("room not found")
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.version, append(json.RawMessage(nil), r.data...), nil
}

func (m *Manager) ApplyUpdate(roomName string, payload json.RawMessage) (int64, json.RawMessage, error) {
	if roomName == "" {
		return 0, nil, errors.New("room name required")
	}
	r := m.ensureRoom(roomName)
	r.mu.Lock()
	defer r.mu.Unlock()

	newVersion := atomic.AddInt64(&r.version, 1)
	r.data = append(json.RawMessage(nil), payload...)

	envelope := map[string]any{
		"type":    "update",
		"room":    roomName,
		"version": newVersion,
		"payload": json.RawMessage(r.data),
	}
	b, err := json.Marshal(envelope)
	if err != nil {
		m.logger.Warn("failed marshal state envelope", zap.Error(err))
		return newVersion, r.data, err
	}

	if m.rm != nil {
		if err := m.rm.Broadcast(roomName, b); err != nil {
			m.logger.Debug("broadcast state failed", zap.Error(err))
		}
	}
	return newVersion, r.data, nil
}
