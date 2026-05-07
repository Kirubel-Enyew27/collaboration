package events

import (
	"collaboration/internal/presence"
	"collaboration/internal/room"
	"collaboration/internal/state"
	"encoding/json"

	"go.uber.org/zap"
)

type Dispatcher struct {
	RM     *room.Manager
	Pres   *presence.Manager
	State  *state.Manager
	Logger *zap.Logger
}

func NewDispatcher(rm *room.Manager, pres *presence.Manager, st *state.Manager, logger *zap.Logger) *Dispatcher {
	return &Dispatcher{RM: rm, Pres: pres, State: st, Logger: logger}
}

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
		if d.Pres != nil {
			d.Pres.MarkActive(p, ev.Room)
		}
		return d.RM.Broadcast(ev.Room, ev.Payload)
	default:
		d.Logger.Warn("unhandled event type", zap.String("type", string(ev.Type)))
	}
	return nil
}

func (d *Dispatcher) DecodePayload(raw json.RawMessage, v any) error {
	return json.Unmarshal(raw, v)
}
