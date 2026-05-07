package handlers

import (
	"collaboration/internal/room"
	"collaboration/internal/ws"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"go.uber.org/zap"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

func NewWSHandler(hub *ws.Hub, rm *room.Manager, logger *zap.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
		if err != nil {
			logger.Warn("failed to upgrade websocket", zap.Error(err))
			c.AbortWithStatus(http.StatusBadRequest)
			return
		}

		client := ws.NewClient(hub, conn, logger)
		hub.Register(client)

		if roomName := c.Query("room"); roomName != "" {
			if err := rm.Join(roomName, client); err != nil {
				logger.Warn("failed to join room", zap.String("room", roomName), zap.Error(err))
			}
		}

		go client.WritePump()
		go client.ReadPump()
	}
}
