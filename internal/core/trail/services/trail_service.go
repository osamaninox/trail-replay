package services

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/osamakhalid/trail-replay/internal/core/trail/domain"
	"github.com/osamakhalid/trail-replay/internal/core/trail/ports/inbound"
	"github.com/osamakhalid/trail-replay/internal/core/trail/ports/outbound"
)

type trailService struct {
	repo outbound.TrailRepository
}

func NewTrailService(repo outbound.TrailRepository) inbound.TrailService {
	return &trailService{repo: repo}
}

func (s *trailService) CreateTrail(ctx context.Context, name string) (*domain.Trail, error) {
	trail := &domain.Trail{
		ID:        uuid.NewString(),
		Name:      name,
		Events:    []domain.Event{},
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
	if err := s.repo.Save(ctx, trail); err != nil {
		return nil, fmt.Errorf("create trail: %w", err)
	}
	return trail, nil
}

func (s *trailService) GetTrail(ctx context.Context, id string) (*domain.Trail, error) {
	trail, err := s.repo.FindByID(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("get trail: %w", err)
	}
	return trail, nil
}

func (s *trailService) AppendEvent(ctx context.Context, trailID string, eventType domain.EventType, payload map[string]any) (*domain.Event, error) {
	trail, err := s.repo.FindByID(ctx, trailID)
	if err != nil {
		return nil, fmt.Errorf("append event: %w", err)
	}
	event := domain.Event{
		ID:        uuid.NewString(),
		Type:      eventType,
		Payload:   payload,
		OccuredAt: time.Now(),
	}
	trail.AddEvent(event)
	if err := s.repo.Update(ctx, trail); err != nil {
		return nil, fmt.Errorf("append event: %w", err)
	}
	added := trail.Events[len(trail.Events)-1]
	return &added, nil
}

func (s *trailService) ReplayTrail(ctx context.Context, trailID string, fromSequence int64) ([]domain.Event, error) {
	trail, err := s.repo.FindByID(ctx, trailID)
	if err != nil {
		return nil, fmt.Errorf("replay trail: %w", err)
	}
	return trail.EventsFrom(fromSequence), nil
}

func (s *trailService) ListTrails(ctx context.Context) ([]domain.Trail, error) {
	trails, err := s.repo.FindAll(ctx)
	if err != nil {
		return nil, fmt.Errorf("list trails: %w", err)
	}
	return trails, nil
}
