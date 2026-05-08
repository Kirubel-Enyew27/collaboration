package metrics

import (
    "time"

    "github.com/prometheus/client_golang/prometheus"
)

var (
    ActiveConnections = prometheus.NewGauge(prometheus.GaugeOpts{
        Name: "collab_active_connections",
        Help: "Number of active websocket connections",
    })

    TotalConnections = prometheus.NewCounter(prometheus.CounterOpts{
        Name: "collab_total_connections",
        Help: "Total websocket connections accepted",
    })

    DroppedMessages = prometheus.NewCounter(prometheus.CounterOpts{
        Name: "collab_dropped_messages_total",
        Help: "Total messages dropped due to full send buffers",
    })

    Rooms = prometheus.NewGauge(prometheus.GaugeOpts{
        Name: "collab_rooms_count",
        Help: "Number of active rooms",
    })

    ParticipantsTotal = prometheus.NewGauge(prometheus.GaugeOpts{
        Name: "collab_participants_total",
        Help: "Total number of participants across all rooms",
    })

    ParticipantsPerRoom = prometheus.NewGaugeVec(prometheus.GaugeOpts{
        Name: "collab_participants_per_room",
        Help: "Participants per room (label=room)",
    }, []string{"room"})

    Broadcasts = prometheus.NewCounterVec(prometheus.CounterOpts{
        Name: "collab_broadcasts_total",
        Help: "Broadcast messages sent per room",
    }, []string{"room"})

    PersistQueueLength = prometheus.NewGauge(prometheus.GaugeOpts{
        Name: "collab_persist_queue_length",
        Help: "Length of the async persist queue",
    })

    PersistDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
        Name:    "collab_persist_duration_seconds",
        Help:    "Duration of persistence operations",
        Buckets: prometheus.DefBuckets,
    })

    PongLatency = prometheus.NewHistogram(prometheus.HistogramOpts{
        Name:    "collab_ws_pong_latency_seconds",
        Help:    "Observed pong latency for websocket clients",
        Buckets: prometheus.ExponentialBuckets(0.001, 2, 15),
    })
)

// Register registers the metrics with the default Prometheus registry.
func Register() {
    prometheus.MustRegister(
        ActiveConnections,
        TotalConnections,
        DroppedMessages,
        Rooms,
        ParticipantsTotal,
        ParticipantsPerRoom,
        Broadcasts,
        PersistQueueLength,
        PersistDuration,
        PongLatency,
    )
}

// Helpers
func IncConnection() { ActiveConnections.Inc(); TotalConnections.Inc() }
func DecConnection() { ActiveConnections.Dec() }
func IncDroppedMessages(n float64) { DroppedMessages.Add(n) }
func SetRooms(n float64) { Rooms.Set(n) }
func IncBroadcast(room string) { Broadcasts.WithLabelValues(room).Inc() }
func SetParticipantsTotal(n float64) { ParticipantsTotal.Set(n) }
func SetParticipantsPerRoom(room string, n float64) { ParticipantsPerRoom.WithLabelValues(room).Set(n) }
func SetPersistQueueLength(n float64) { PersistQueueLength.Set(n) }
func ObservePersistDuration(d time.Duration) { PersistDuration.Observe(d.Seconds()) }
func ObservePongLatency(d time.Duration) { PongLatency.Observe(d.Seconds()) }
