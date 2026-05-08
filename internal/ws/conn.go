package ws

import (
	"collaboration/internal/events"
	"collaboration/internal/metrics"
	"collaboration/internal/room"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"go.uber.org/zap"
)

const (
	writeWait      = 10 * time.Second
	pongWait       = 60 * time.Second
	pingPeriod     = (pongWait * 9) / 10
	maxMessageSize = 512
)

// Client represents a single WebSocket connection.
type Client struct {
	ID      string
	Hub     *Hub
	Conn    *websocket.Conn
	Send    chan []byte
	Logger  *zap.Logger
	Room    string
	OnClose func(*Client)
	ED      *events.Dispatcher
	Pres    interface {
		MarkActive(room.Participant, string)
		MarkOnline(room.Participant, string)
		MarkOffline(room.Participant, string)
	}
	// heartbeat and slow-client detection
	lastPong  time.Time
	lastPing  time.Time
	dropCount int
	rooms     map[string]struct{}
	done      chan struct{}
	mu        sync.Mutex
	closeOnce sync.Once
}

// GetID returns the client's unique identifier (satisfies room.Participant).
func (c *Client) GetID() string { return c.ID }

// NewClient constructs a client and registers it with the hub.
func NewClient(hub *Hub, conn *websocket.Conn, logger *zap.Logger, ed *events.Dispatcher, pres interface {
	MarkActive(room.Participant, string)
	MarkOnline(room.Participant, string)
	MarkOffline(room.Participant, string)
}) *Client {
	return &Client{
		ID:       uuid.New().String(),
		Hub:      hub,
		Conn:     conn,
		Send:     make(chan []byte, 256),
		Logger:   logger,
		ED:       ed,
		Pres:     pres,
		lastPong: time.Now(),
		rooms:    make(map[string]struct{}),
		done:     make(chan struct{}),
	}
}

// SendMessage attempts to enqueue a message to the client without blocking.
func (c *Client) SendMessage(b []byte) {
	select {
	case <-c.done:
		return
	default:
	}

	select {
	case <-c.done:
		return
	case c.Send <- b:
		// good
	default:
		// slow client — increment drop counter and disconnect if too many
		c.mu.Lock()
		c.dropCount++
		drops := c.dropCount
		c.mu.Unlock()
		c.Logger.Warn("dropping message to client; send buffer full", zap.String("client", c.ID), zap.Int("drops", drops))
		metrics.IncDroppedMessages(1)
		if drops > 50 {
			c.Logger.Info("client appears too slow; closing connection", zap.String("client", c.ID))
			c.Close()
		}
	}
}

// ReadPump reads messages from the WebSocket connection.
func (c *Client) ReadPump() {
	defer func() {
		c.Hub.Unregister(c)
		if c.OnClose != nil {
			c.OnClose(c)
		}
		c.Close()
	}()

	c.Conn.SetReadLimit(maxMessageSize)
	_ = c.Conn.SetReadDeadline(time.Now().Add(pongWait))
	c.Conn.SetPongHandler(func(appData string) error {
		_ = c.Conn.SetReadDeadline(time.Now().Add(pongWait))
		now := time.Now()
		c.mu.Lock()
		c.lastPong = now
		// observe latency if we have a ping timestamp
		if !c.lastPing.IsZero() {
			lat := now.Sub(c.lastPing)
			metrics.ObservePongLatency(lat)
		}
		c.mu.Unlock()
		return nil
	})

	for {
		_, message, err := c.Conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				c.Logger.Warn("unexpected websocket close", zap.Error(err))
			}
			break
		}
		// Update presence activity
		if c.Pres != nil {
			// Attempt to mark active; ignore if participant interface mismatch
			// room name may be empty; callers will provide it when known
			_ = tryMarkActive(c.Pres, c, c.CurrentRoom())
		}

		// Dispatch event if dispatcher available
		if c.ED != nil {
			if err := c.ED.Dispatch(c, message); err != nil {
				c.Logger.Warn("dispatch error", zap.Error(err))
			}
			continue
		}
		// Fallback: just log incoming messages
		c.Logger.Debug("received message", zap.Int("len", len(message)), zap.String("client", c.ID))
	}
}

// WritePump sends messages from the send channel to the WebSocket connection.
func (c *Client) WritePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		c.Conn.Close()
	}()

	for {
		select {
		case <-c.done:
			_ = c.Conn.SetWriteDeadline(time.Now().Add(writeWait))
			_ = c.Conn.WriteMessage(websocket.CloseMessage, []byte{})
			return
		case message, ok := <-c.Send:
			_ = c.Conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				// Hub closed the channel
				_ = c.Conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			// Batch messages to reduce syscalls: write first message, then drain
			w, err := c.Conn.NextWriter(websocket.TextMessage)
			if err != nil {
				return
			}
			if _, err := w.Write(message); err != nil {
				_ = w.Close()
				return
			}

			// drain additional queued messages into same writer
		drainLoop:
			for {
				select {
				case <-c.done:
					_ = w.Close()
					return
				case msg, ok := <-c.Send:
					if !ok {
						_ = w.Close()
						return
					}
					// separate messages by newline so clients can split if needed
					if _, err := w.Write([]byte("\n")); err != nil {
						break drainLoop
					}
					if _, err := w.Write(msg); err != nil {
						break drainLoop
					}
				default:
					break drainLoop
				}
			}
			if err := w.Close(); err != nil {
				return
			}
		case <-ticker.C:
			_ = c.Conn.SetWriteDeadline(time.Now().Add(writeWait))
			// set ping timestamp
			c.mu.Lock()
			c.lastPing = time.Now()
			c.mu.Unlock()
			if err := c.Conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
			// if no pong received within 2*pongWait, close
			c.mu.Lock()
			if time.Since(c.lastPong) > 2*pongWait {
				c.mu.Unlock()
				c.Logger.Info("no pong seen recently; closing connection", zap.String("client", c.ID))
				return
			}
			c.mu.Unlock()
		}
	}
}

// TrackRoom records room membership for disconnect cleanup.
func (c *Client) TrackRoom(roomName string) {
	if roomName == "" {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.rooms[roomName] = struct{}{}
	c.Room = roomName
}

// UntrackRoom removes room membership from the client.
func (c *Client) UntrackRoom(roomName string) {
	if roomName == "" {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.rooms, roomName)
	if c.Room == roomName {
		c.Room = ""
		for name := range c.rooms {
			c.Room = name
			break
		}
	}
}

// Rooms returns a snapshot of the rooms the client has joined.
func (c *Client) Rooms() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	rooms := make([]string, 0, len(c.rooms))
	for roomName := range c.rooms {
		rooms = append(rooms, roomName)
	}
	return rooms
}

// CurrentRoom returns the room currently used for presence activity.
func (c *Client) CurrentRoom() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.Room
}

// Close cleanly signals the pumps to exit and unblocks pending reads.
func (c *Client) Close() {
	c.closeOnce.Do(func() {
		close(c.done)
		_ = c.Conn.Close()
	})
}

// helper to call MarkActive safely via reflection-like interface assertion
func tryMarkActive(pres interface{}, participant interface{}, roomName string) bool {
	type marker interface {
		MarkActive(interface{}, string)
	}
	if m, ok := pres.(marker); ok {
		m.MarkActive(participant.(room.Participant), roomName)
		return true
	}
	// try concrete type with correct signature
	type m2 interface {
		MarkActive(room.Participant, string)
	}
	if m, ok := pres.(m2); ok {
		m.MarkActive(participant.(room.Participant), roomName)
		return true
	}
	return false
}
