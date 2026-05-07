package events

import (
	"collaboration/internal/presence"
	"collaboration/internal/room"
	"encoding/json"

	"go.uber.org/zap"
)

// Dispatcher routes events to the appropriate handlers (rooms, broadcasts, etc.).
type Dispatcher struct {
	RM     *room.Manager
	Pres   *presence.Manager
	Logger *zap.Logger
}

// NewDispatcher creates an event dispatcher.
func NewDispatcher(rm *room.Manager, pres *presence.Manager, logger *zap.Logger) *Dispatcher {
	return &Dispatcher{RM: rm, Pres: pres, Logger: logger}
}

// Dispatch parses and handles a raw incoming message for the given participant.
func (d *Dispatcher) Dispatch(p room.Participant, raw []byte) error {
	ev, err := Parse(raw)
	if err != nil {
		d.Logger.Warn("invalid event", zap.Error(err))
		return err
	}

	pid := p.GetID()
	switch ev.Type {
	case EventJoin:
		d.Logger.Debug("dispatch join", zap.String("room", ev.Room), zap.String("participant", pid))
		if err := d.RM.Join(ev.Room, p); err != nil {
			return err
		}
		if d.Pres != nil {
			d.Pres.MarkOnline(p, ev.Room)
		}
		return nil
	case EventLeave:
		d.Logger.Debug("dispatch leave", zap.String("room", ev.Room), zap.String("participant", pid))
		if err := d.RM.Leave(ev.Room, p); err != nil {
			return err
		}
		if d.Pres != nil {
			d.Pres.MarkOffline(p, ev.Room)
		}
		return nil
	case EventUpdate:
		d.Logger.Debug("dispatch update", zap.String("room", ev.Room), zap.String("participant", pid))
		// Forward payload to room participants
		if d.Pres != nil {
			d.Pres.MarkActive(p, ev.Room)
		}
		return d.RM.Broadcast(ev.Room, ev.Payload)
	case EventBroadcast:
		d.Logger.Debug("dispatch broadcast", zap.String("room", ev.Room), zap.String("participant", pid))
		// Broadcast behaves same as update for now
		if d.Pres != nil {
			d.Pres.MarkActive(p, ev.Room)
		}
		return d.RM.Broadcast(ev.Room, ev.Payload)
	default:
		d.Logger.Warn("unhandled event type", zap.String("type", string(ev.Type)))
	}
	return nil
}

// Helper: decode payload into a target structure
func (d *Dispatcher) DecodePayload(raw json.RawMessage, v any) error {
	return json.Unmarshal(raw, v)
}
