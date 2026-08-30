package domain

import (
	"time"
)

type EventType string

const (
	EventTypeCreated  EventType = "created"
	EventTypeUpdated  EventType = "updated"
	EventTypeDeleted  EventType = "deleted"
	EventTypeReplayed EventType = "replayed"
)

type Event struct {
	ID        string
	TrailID   string
	Type      EventType
	Payload   map[string]any
	OccuredAt time.Time
	Sequence  int64
}

type Trail struct {
	ID        string
	Name      string
	Events    []Event
	CreatedAt time.Time
	UpdatedAt time.Time
}

func (t *Trail) AddEvent(e Event) {
	e.TrailID = t.ID
	e.Sequence = int64(len(t.Events)) + 1
	t.Events = append(t.Events, e)
	t.UpdatedAt = time.Now()
}

func (t *Trail) EventsFrom(sequence int64) []Event {
	var result []Event
	for _, e := range t.Events {
		if e.Sequence >= sequence {
			result = append(result, e)
		}
	}
	return result
}
