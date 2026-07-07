package inbound

import (
	"context"

	"github.com/osamakhalid/trail-replay/internal/core/trail/domain"
)

// TrailService is the driving port — defines operations the application exposes.
type TrailService interface {
	CreateTrail(ctx context.Context, name string) (*domain.Trail, error)
	GetTrail(ctx context.Context, id string) (*domain.Trail, error)
	AppendEvent(ctx context.Context, trailID string, eventType domain.EventType, payload map[string]any) (*domain.Event, error)
	ReplayTrail(ctx context.Context, trailID string, fromSequence int64) ([]domain.Event, error)
	ListTrails(ctx context.Context) ([]domain.Trail, error)
}
