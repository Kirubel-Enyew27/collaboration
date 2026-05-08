package presence

import (
	"encoding/json"
	"sync"
	"time"

	"collaboration/internal/room"

	"go.uber.org/zap"
)

type Status string

const (
	StatusOnline  Status = "online"
	StatusOffline Status = "offline"
	StatusIdle    Status = "idle"
)

type record struct {
	participant room.Participant
	room        string
	status      Status
	lastActive  time.Time
}

// Manager tracks presence state and broadcasts presence updates to rooms.
type Manager struct {
	mu           sync.RWMutex
	records      map[string]*record
	rm           *room.Manager
	logger       *zap.Logger
	idleTimeout  time.Duration
	staleTimeout time.Duration
	ticker       *time.Ticker
	stop         chan struct{}
}

// NewManager creates a presence manager.
func NewManager(rm *room.Manager, logger *zap.Logger) *Manager {
	return &Manager{
		records:      make(map[string]*record),
		rm:           rm,
		logger:       logger,
		idleTimeout:  2 * time.Minute,
		staleTimeout: 10 * time.Minute,
		stop:         make(chan struct{}),
	}
}

// Start begins the background idle/stale checker.
func (m *Manager) Start() {
	if m.ticker != nil {
		return
	}
	m.ticker = time.NewTicker(30 * time.Second)
	go m.run()
}

// Shutdown stops the background checker.
func (m *Manager) Shutdown() {
	if m.ticker != nil {
		m.ticker.Stop()
	}
	close(m.stop)
}

func (m *Manager) run() {
	for {
		select {
		case <-m.ticker.C:
			m.check()
		case <-m.stop:
			return
		}
	}
}

func (m *Manager) check() {
	now := time.Now()
	var toOffline []string
	m.mu.Lock()
	for id, rec := range m.records {
		if rec.status != StatusIdle && now.Sub(rec.lastActive) > m.idleTimeout {
			rec.status = StatusIdle
			m.publish(rec)
		}
		if now.Sub(rec.lastActive) > m.staleTimeout {
			toOffline = append(toOffline, id)
		}
	}
	for _, id := range toOffline {
		if rec, ok := m.records[id]; ok {
			rec.status = StatusOffline
			m.publish(rec)
			delete(m.records, id)
			m.logger.Info("removed stale participant", zap.String("participant", id))
		}
	}
	m.mu.Unlock()
}

// MarkOnline registers participant as online and broadcasts presence.
func (m *Manager) MarkOnline(p room.Participant, roomName string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	id := p.GetID()
	rec, ok := m.records[id]
	if !ok {
		rec = &record{participant: p}
		m.records[id] = rec
	}
	rec.room = roomName
	rec.status = StatusOnline
	rec.lastActive = time.Now()
	m.publish(rec)
}

// MarkOffline marks the participant offline and broadcasts presence.
func (m *Manager) MarkOffline(p room.Participant, roomName string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	id := p.GetID()
	rec, ok := m.records[id]
	if ok {
		rec.status = StatusOffline
		rec.lastActive = time.Now()
		rec.room = roomName
		m.publish(rec)
		delete(m.records, id)
	}
}

// MarkActive updates the lastActive timestamp and broadcasts if needed.
func (m *Manager) MarkActive(p room.Participant, roomName string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	id := p.GetID()
	rec, ok := m.records[id]
	if !ok {
		rec = &record{participant: p, room: roomName}
		m.records[id] = rec
	}
	rec.lastActive = time.Now()
	if rec.status != StatusOnline {
		rec.status = StatusOnline
	}
	m.publish(rec)
}

// publish sends a presence update to the participant's room via room.Manager.Broadcast.
func (m *Manager) publish(r *record) {
	if r.room == "" || m.rm == nil {
		return
	}
	payload := map[string]interface{}{
		"type":       "presence",
		"user":       r.participant.GetID(),
		"status":     string(r.status),
		"lastActive": r.lastActive.UTC().Format(time.RFC3339),
	}
	b, err := json.Marshal(payload)
	if err != nil {
		m.logger.Warn("failed marshal presence payload", zap.Error(err))
		return
	}
	if err := m.rm.Broadcast(r.room, b); err != nil {
		m.logger.Debug("broadcast presence failed", zap.Error(err))
	}
}
