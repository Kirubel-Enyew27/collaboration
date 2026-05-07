package handlers

import (
	"net/http"

	"collaboration/internal/events"
	"collaboration/internal/presence"
	"collaboration/internal/room"
	"collaboration/internal/ws"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"go.uber.org/zap"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		// TODO: use proper origin checks for production
		return true
	},
}

// NewWSHandler returns a Gin handler that upgrades HTTP requests to WebSocket and
// manages the client lifecycle with the provided hub.
func NewWSHandler(hub *ws.Hub, rm *room.Manager, ed *events.Dispatcher, pres *presence.Manager, logger *zap.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
		if err != nil {
			logger.Warn("failed to upgrade websocket", zap.Error(err))
			c.AbortWithStatus(http.StatusBadRequest)
			return
		}

		client := ws.NewClient(hub, conn, logger, ed, pres)
		hub.Register(client)

		if pres != nil {
			pres.MarkOnline(client, "")
		}

		// If a room query parameter is provided, auto-join that room
		if roomName := c.Query("room"); roomName != "" {
			if err := rm.Join(roomName, client); err != nil {
				logger.Warn("failed to join room", zap.String("room", roomName), zap.Error(err))
			} else {
				// ensure the client leaves the room on close
				prev := client.OnClose
				client.OnClose = func(cc *ws.Client) {
					_ = rm.Leave(roomName, cc)
					if pres != nil {
						pres.MarkOffline(cc, roomName)
					}
					if prev != nil {
						prev(cc)
					}
				}
				if pres != nil {
					pres.MarkOnline(client, roomName)
				}
			}
		}

		// Start read and write pumps
		go client.WritePump()
		go client.ReadPump()
	}
}
