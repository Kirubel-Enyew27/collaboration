package handlers

import (
    "net/http"

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
func NewWSHandler(hub *ws.Hub, logger *zap.Logger) gin.HandlerFunc {
    return func(c *gin.Context) {
        conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
        if err != nil {
            logger.Warn("failed to upgrade websocket", zap.Error(err))
            c.AbortWithStatus(http.StatusBadRequest)
            return
        }

        client := ws.NewClient(hub, conn, logger)
        hub.Register(client)

        // Start read and write pumps
        go client.WritePump()
        go client.ReadPump()
    }
}
