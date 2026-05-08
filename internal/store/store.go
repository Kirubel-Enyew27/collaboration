package store

import (
	"context"
	"time"
)

type Event struct {
	ID        int64
	Type      string
	Room      string
	Payload   []byte
	Version   int64
	CreatedAt time.Time
}

type RoomMeta struct {
	Name      string
	CreatedAt time.Time
}

type RoomRepository interface {
	CreateRoom(ctx context.Context, name string) error
	GetRoom(ctx context.Context, name string) (*RoomMeta, error)
	DeleteRoom(ctx context.Context, name string) error
	ListRooms(ctx context.Context) ([]RoomMeta, error)
}

type EventRepository interface {
	AppendEvent(ctx context.Context, e *Event) (int64, error)
	ListEventsByRoom(ctx context.Context, room string, limit int) ([]Event, error)
}
