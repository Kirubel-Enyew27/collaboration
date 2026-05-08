package ws

import (
	"collaboration/internal/events"
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
	lastLong  time.Time
	dropCount int
	mu        sync.Mutex
	closeOnce sync.Once
}

func (c *Client) GetID() string { return c.ID }

func NewClient(hub *Hub, conn *websocket.Conn, logger *zap.Logger,
	ed *events.Dispatcher, pres interface {
		MarkActive(room.Participant, string)
		MarkOnline(room.Participant, string)
		MarkOffline(room.Participant, string)
	}) *Client {
	return &Client{
		ID:     uuid.NewString(),
		Hub:    hub,
		Conn:   conn,
		Send:   make(chan []byte, 256),
		Logger: logger,
		ED:     ed,
		Pres:   pres,
	}
}

func (c *Client) SendMessage(b []byte) {
	select {
	case c.Send <- b:
	default:
		c.mu.Lock()
		c.dropCount++
		drops := c.dropCount
		c.mu.Unlock()
		c.Logger.Warn("dropping message to client; send buffer full", zap.String("client", c.ID), zap.Int("drops", drops))
		if drops > 0 {
			c.Logger.Info("client appears too low; closing connection", zap.String("client", c.ID))
			c.Close()
		}
	}
}

func (c *Client) ReadPump() {
	defer func() {
		c.Hub.Unregister(c)
		if c.OnClose != nil {
			c.OnClose(c)
		}
		c.Conn.Close()
	}()

	c.Conn.SetReadLimit(maxMessageSize)
	_ = c.Conn.SetReadDeadline(time.Now().Add(pongWait))
	c.Conn.SetPongHandler(func(appData string) error {
		_ = c.Conn.SetReadDeadline(time.Now().Add(pongWait))
		c.mu.Lock()
		c.lastLong = time.Now()
		c.mu.Unlock()
		return nil
	})

	for {
		_, message, err := c.Conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway,
				websocket.CloseAbnormalClosure) {
				c.Logger.Warn("unexpected websocket close", zap.Error(err))
			}
			break
		}
		if c.Pres != nil {
			_ = tryMarkActive(c.Pres, c, c.Room)
		}

		if c.ED != nil {
			if err := c.ED.Dispatch(c, message); err != nil {
				c.Logger.Warn("dispatch error", zap.Error(err))
			}
			continue
		}

		c.Logger.Debug("received message", zap.Int("len", len(message)))
		zap.String("client", c.ID)
	}
}

func (c *Client) WritePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		c.Conn.Close()
	}()

	for {
		select {
		case message, ok := <-c.Send:
			_ = c.Conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				_ = c.Conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			w, err := c.Conn.NextWriter(websocket.TextMessage)
			if err != nil {
				return
			}
			if _, err := w.Write(message); err != nil {
				_ = w.Close()
				return
			}

		drainLoop:
			for {
				select {
				case msg := <-c.Send:
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
			if err := c.Conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
			c.mu.Lock()
			if time.Since(c.lastLong) > 2*pongWait {
				c.mu.Unlock()
				c.Logger.Info("no pong seen recently; closing connection", zap.String("client", c.ID))
				return
			}
			c.mu.Unlock()
		}
	}
}

func (c *Client) Close() {
	c.closeOnce.Do(func() {

		select {
		case <-c.Hub.done:
		default:
		}
		close(c.Send)
	})
}

func tryMarkActive(pres interface{}, participant interface{}, roomName string) bool {
	type marker interface {
		MarkActive(interface{}, string)
	}
	if m, ok := pres.(marker); ok {
		m.MarkActive(participant.(room.Participant), roomName)
		return true
	}
	type m2 interface {
		MarkActive(room.Participant, string)
	}
	if m, ok := pres.(m2); ok {
		m.MarkActive(participant.(room.Participant), roomName)
		return true
	}
	return false
}
