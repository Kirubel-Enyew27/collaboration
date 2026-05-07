package events

import (
	"encoding/json"
	"errors"
)

type EventType string

const (
	EventJoin      EventType = "join"
	EventLeave     EventType = "leave"
	EventUpdate    EventType = "update"
	EventBroadcast EventType = "broadcast"
)

type Event struct {
	Type    EventType       `json:"type"`
	Room    string          `json:"room,omitempty"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

func Parse(raw []byte) (*Event, error) {
	var e Event
	if err := json.Unmarshal(raw, &e); err != nil {
		return nil, err
	}
	if e.Type == "" {
		return nil, errors.New("missing event type")
	}
	switch e.Type {
	case EventJoin, EventLeave, EventUpdate, EventBroadcast:
		if (e.Type == EventJoin || e.Type == EventLeave || e.Type == EventUpdate ||
		e.Type == EventBroadcast) && e.Room == "" {
			return nil, errors.New("room required")
		}
	default:
		return nil, errors.New("unknown event type")
	}
	return &e, nil
}
