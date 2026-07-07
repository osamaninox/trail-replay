package storage

import (
	"context"
	"fmt"
	"sync"

	"github.com/osamakhalid/trail-replay/internal/core/trail/domain"
)

type inMemoryRepository struct {
	mu     sync.RWMutex
	trails map[string]*domain.Trail
}

func NewInMemoryRepository() *inMemoryRepository {
	return &inMemoryRepository{
		trails: make(map[string]*domain.Trail),
	}
}

func (r *inMemoryRepository) Save(ctx context.Context, trail *domain.Trail) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.trails[trail.ID]; exists {
		return fmt.Errorf("trail %s already exists", trail.ID)
	}
	cp := *trail
	r.trails[trail.ID] = &cp
	return nil
}

func (r *inMemoryRepository) FindByID(ctx context.Context, id string) (*domain.Trail, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	trail, ok := r.trails[id]
	if !ok {
		return nil, fmt.Errorf("trail %s not found", id)
	}
	cp := *trail
	return &cp, nil
}

func (r *inMemoryRepository) FindAll(ctx context.Context) ([]domain.Trail, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	trails := make([]domain.Trail, 0, len(r.trails))
	for _, t := range r.trails {
		trails = append(trails, *t)
	}
	return trails, nil
}

func (r *inMemoryRepository) Update(ctx context.Context, trail *domain.Trail) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.trails[trail.ID]; !ok {
		return fmt.Errorf("trail %s not found", trail.ID)
	}
	cp := *trail
	r.trails[trail.ID] = &cp
	return nil
}
