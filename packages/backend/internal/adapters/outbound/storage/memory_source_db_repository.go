package storage

import (
	"context"
	"fmt"
	"sync"

	"github.com/google/uuid"

	"trail-replay/internal/core/trail/domain"
)

type inMemorySourceDatabaseRepository struct {
	mu  sync.RWMutex
	dbs map[uuid.UUID]*domain.SourceDatabase
}

func NewInMemorySourceDatabaseRepository() *inMemorySourceDatabaseRepository {
	return &inMemorySourceDatabaseRepository{
		dbs: make(map[uuid.UUID]*domain.SourceDatabase),
	}
}

func (r *inMemorySourceDatabaseRepository) Save(ctx context.Context, db *domain.SourceDatabase) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.dbs[db.ID]; exists {
		return fmt.Errorf("source database %s already exists", db.ID)
	}
	cp := *db
	r.dbs[db.ID] = &cp
	return nil
}

func (r *inMemorySourceDatabaseRepository) FindByID(ctx context.Context, id uuid.UUID) (*domain.SourceDatabase, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	db, ok := r.dbs[id]
	if !ok {
		return nil, fmt.Errorf("source database %s not found", id)
	}
	cp := *db
	return &cp, nil
}

func (r *inMemorySourceDatabaseRepository) FindAllByUser(ctx context.Context, userID uuid.UUID) ([]domain.SourceDatabase, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	result := make([]domain.SourceDatabase, 0)
	for _, db := range r.dbs {
		if db.UserID == userID {
			result = append(result, *db)
		}
	}
	return result, nil
}

func (r *inMemorySourceDatabaseRepository) Update(ctx context.Context, db *domain.SourceDatabase) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.dbs[db.ID]; !ok {
		return fmt.Errorf("source database %s not found", db.ID)
	}
	cp := *db
	r.dbs[db.ID] = &cp
	return nil
}

func (r *inMemorySourceDatabaseRepository) Delete(ctx context.Context, id uuid.UUID) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.dbs[id]; !ok {
		return fmt.Errorf("source database %s not found", id)
	}
	delete(r.dbs, id)
	return nil
}
