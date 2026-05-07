package state

import (
	"context"
	"encoding/json"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"collaboration/internal/metrics"
	"collaboration/internal/room"
	"collaboration/internal/store"

	"go.uber.org/zap"
)

// RoomState holds state and version for a room.
type RoomState struct {
    mu      sync.RWMutex
    version int64
    data    json.RawMessage
}

// Manager manages shared state per room in a thread-safe way.
type Manager struct {
    mu     sync.RWMutex
    rooms  map[string]*RoomState
    rm     *room.Manager
    store  store.EventRepository
    logger *zap.Logger
    // async persistence
    persistQueue chan *store.Event
    workers      int
    wg           sync.WaitGroup
}

// NewManager creates a state manager that will use room.Manager for broadcasts.
// Optionally accepts an EventRepository to persist updates.
func NewManager(rm *room.Manager, logger *zap.Logger, repo store.EventRepository) *Manager {
    m := &Manager{rooms: make(map[string]*RoomState), rm: rm, store: repo, logger: logger}
    if repo != nil {
        // configure worker count and queue size
        workers := runtime.NumCPU()
        if workers < 1 {
            workers = 1
        }
        m.workers = workers
        m.persistQueue = make(chan *store.Event, 1024)
        // start workers
        for i := 0; i < m.workers; i++ {
            m.wg.Add(1)
            go func() {
                defer m.wg.Done()
                for ev := range m.persistQueue {
                    start := time.Now()
                    if _, err := m.store.AppendEvent(context.Background(), ev); err != nil {
                        m.logger.Warn("worker failed persist event", zap.Error(err))
                    }
                    metrics.ObservePersistDuration(time.Since(start))
                    // update queue length metric
                    metrics.SetPersistQueueLength(float64(len(m.persistQueue)))
                }
            }()
        }
    }
    return m
}

// Shutdown stops background persistence workers and waits for flush.
func (m *Manager) Shutdown() {
    if m.persistQueue == nil {
        return
    }
    // close queue and wait for workers to finish
    close(m.persistQueue)
    m.wg.Wait()
}

// ensureRoom gets or creates a RoomState.
func (m *Manager) ensureRoom(name string) *RoomState {
    m.mu.RLock()
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

// GetState returns the current version and data for a room.
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

// ApplyUpdate replaces the room state with the provided payload, increments
// the server-side version, persists the event (if repo available), and
// broadcasts the new state to room members.
func (m *Manager) ApplyUpdate(roomName string, payload json.RawMessage) (int64, json.RawMessage, error) {
    if roomName == "" {
        return 0, nil, errors.New("room name required")
    }
    r := m.ensureRoom(roomName)
    r.mu.Lock()
    defer r.mu.Unlock()

    // simple replace strategy: server assigns next version and stores payload
    newVersion := atomic.AddInt64(&r.version, 1)
    r.data = append(json.RawMessage(nil), payload...)

    // enqueue persist event if store provided
    if m.store != nil && m.persistQueue != nil {
        ev := &store.Event{Type: "update", Room: roomName, Payload: r.data, Version: newVersion}
        select {
        case m.persistQueue <- ev:
            // enqueued
            metrics.SetPersistQueueLength(float64(len(m.persistQueue)))
        default:
            m.logger.Warn("persist queue full, dropping persist event", zap.String("room", roomName))
            metrics.SetPersistQueueLength(float64(len(m.persistQueue)))
        }
    }

    // prepare broadcast envelope
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

    // broadcast to room members
    if m.rm != nil {
        if err := m.rm.Broadcast(roomName, b); err != nil {
            m.logger.Debug("broadcast state failed", zap.Error(err))
        }
    }
    return newVersion, r.data, nil
}
