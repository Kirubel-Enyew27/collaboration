package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"collaboration/internal/handlers"
	"collaboration/internal/logger"
	"collaboration/internal/room"
	"collaboration/internal/ws"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

func main() {
	// Initialize logger
	log := logger.NewLogger()
	defer log.Sync()

	// Create hub and run it
	hub := ws.NewHub(log)
	go hub.Run()

	// Room manager
	rm := room.NewManager(log)

	// Setup Gin
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery())

	// Health endpoint
	r.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	// WebSocket endpoint (supports ?room=)
	r.GET("/ws", handlers.NewWSHandler(hub, rm, log))

	// Room management endpoints
	r.POST("/rooms", func(c *gin.Context) {
		var body struct {
			Name string `json:"name"`
		}
		if err := c.BindJSON(&body); err != nil || body.Name == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "name required"})
			return
		}
		if err := rm.CreateRoom(body.Name); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.Status(http.StatusCreated)
	})

	r.GET("/rooms/:room/participants", func(c *gin.Context) {
		roomName := c.Param("room")
		ids, err := rm.Participants(roomName)
		if err != nil {
			c.JSON(http.StatusNotFound, gin.H{"error": "room not found"})
			return
		}
		c.JSON(http.StatusOK, gin.H{"participants": ids})
	})

	srv := &http.Server{
		Addr:    ":8080",
		Handler: r,
	}

	// Start server
	go func() {
		log.Info("starting server", zap.String("addr", srv.Addr))
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal("server error", zap.Error(err))
		}
	}()

	// Graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Info("shutting down server")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		log.Error("server forced to shutdown", zap.Error(err))
	}

	// Close hub and all connections
	hub.Shutdown()

	log.Info("server exiting")
}
