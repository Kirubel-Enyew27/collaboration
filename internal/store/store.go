package store

import (
    "context"
    "time"
)

// Event represents a persisted collaboration event.
type Event struct {
    ID        int64
    Type      string
    Room      string
    Payload   []byte
    Version   int64
    CreatedAt time.Time
}

// RoomMeta represents persisted room metadata.
type RoomMeta struct {
    Name      string
    CreatedAt time.Time
}

// RoomRepository persists room metadata.
type RoomRepository interface {
    CreateRoom(ctx context.Context, name string) error
    GetRoom(ctx context.Context, name string) (*RoomMeta, error)
    DeleteRoom(ctx context.Context, name string) error
    ListRooms(ctx context.Context) ([]RoomMeta, error)
}

// EventRepository persists events.
type EventRepository interface {
    AppendEvent(ctx context.Context, e *Event) (int64, error)
    ListEventsByRoom(ctx context.Context, room string, limit int) ([]Event, error)
}
