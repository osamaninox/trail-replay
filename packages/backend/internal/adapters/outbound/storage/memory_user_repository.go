package storage

import (
	"context"
	"fmt"
	"sync"

	"github.com/google/uuid"

	"trail-replay/internal/core/trail/domain"
)

type inMemoryUserRepository struct {
	mu    sync.RWMutex
	users map[uuid.UUID]*domain.User
}

func NewInMemoryUserRepository() *inMemoryUserRepository {
	return &inMemoryUserRepository{
		users: make(map[uuid.UUID]*domain.User),
	}
}

func (r *inMemoryUserRepository) Save(ctx context.Context, user *domain.User) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, u := range r.users {
		if u.Email == user.Email {
			return fmt.Errorf("email %s already registered", user.Email)
		}
	}
	if _, exists := r.users[user.ID]; exists {
		return fmt.Errorf("user %s already exists", user.ID)
	}
	cp := *user
	r.users[user.ID] = &cp
	return nil
}

func (r *inMemoryUserRepository) FindByEmail(ctx context.Context, email string) (*domain.User, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, u := range r.users {
		if u.Email == email {
			cp := *u
			return &cp, nil
		}
	}
	return nil, fmt.Errorf("user with email %s not found", email)
}

func (r *inMemoryUserRepository) FindByID(ctx context.Context, id uuid.UUID) (*domain.User, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	user, ok := r.users[id]
	if !ok {
		return nil, fmt.Errorf("user %s not found", id)
	}
	cp := *user
	return &cp, nil
}
