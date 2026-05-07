package ws

import (
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
}

func NewClient(hub *Hub, conn *websocket.Conn, logger *zap.Logger) *Client {
	return &Client{
		ID:     uuid.NewString(),
		Hub:    hub,
		Conn:   conn,
		Send:   make(chan []byte, 256),
		Logger: logger,
	}
}

func (c *Client) SendMessage(b []byte) {
	select {
	case c.Send <- b:
	default:
		c.Logger.Warn("dropping message to client; send buffer full", zap.String("client", c.ID))
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
	c.Conn.SetPongHandler(func(string) error {
		_ = c.Conn.SetReadDeadline(time.Now().Add(pongWait))
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
		c.Logger.Debug("received message", zap.Int("len", len(message)))
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
				return
			}
			if err := w.Close(); err != nil {
				return
			}
		case <-ticker.C:
			_ = c.Conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.Conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

func (c *Client) Close() {
	select {
	case <-c.Hub.done:
	default:
	}
	close(c.Send)
}
