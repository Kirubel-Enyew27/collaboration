package main

import (
	"collaboration/internal/auth"
	"collaboration/internal/events"
	"collaboration/internal/handlers"
	"collaboration/internal/logger"
	"collaboration/internal/presence"
	"collaboration/internal/room"
	"collaboration/internal/state"
	"collaboration/internal/store/sqlite"
	"collaboration/internal/ws"
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

func main() {
	log := logger.NewLogger()
	defer log.Sync()

	hub := ws.NewHub(log)
	go hub.Run()

	sqldb, err := sqlite.NewSQLiteStore("./data.db")
	if err != nil {
		log.Fatal("failed to open sqlite store", zap.Error(err))
	}
	defer sqldb.Close()

	rm := room.NewManager(log, sqldb)

	pres := presence.NewManager(rm, log)
	pres.Start()

	st := state.NewManager(rm, log, sqldb)

	ed := events.NewDispatcher(rm, pres, st, log)

	apiKeys := map[string]string{}
	apiKeys["dev-key"] = "dev-user"
	authz := auth.New(apiKeys, log)

	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery())

	authGroup := r.Group("/", authz.Middleware())

	authGroup.GET("/ws", handlers.NewWSHandler(hub, rm, ed, pres, log))

	authGroup.POST("/rooms", func(c *gin.Context) {
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

	authGroup.GET("/rooms/:room/participants", func(c *gin.Context) {
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

	go func() {
		log.Info("starting server", zap.String("addr", srv.Addr))
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal("server error", zap.Error(err))
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Info("shutting down server")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		log.Error("server forced to shutdown", zap.Error(err))
	}

	hub.Shutdown()

	pres.Shutdown()

	st.Shutdown()

	log.Info("server exiting")
}
