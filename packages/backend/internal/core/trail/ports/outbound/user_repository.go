package outbound

import (
	"context"

	"github.com/google/uuid"

	"trail-replay/internal/core/trail/domain"
)

// UserRepository is the driven port for user persistence.
type UserRepository interface {
	Save(ctx context.Context, user *domain.User) error
	FindByEmail(ctx context.Context, email string) (*domain.User, error)
	FindByID(ctx context.Context, id uuid.UUID) (*domain.User, error)
}
