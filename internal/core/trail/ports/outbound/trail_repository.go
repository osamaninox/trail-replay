package outbound

import (
	"context"

	"github.com/osamakhalid/trail-replay/internal/core/trail/domain"
)

// TrailRepository is the driven port — abstracts persistence from the core.
type TrailRepository interface {
	Save(ctx context.Context, trail *domain.Trail) error
	FindByID(ctx context.Context, id string) (*domain.Trail, error)
	FindAll(ctx context.Context) ([]domain.Trail, error)
	Update(ctx context.Context, trail *domain.Trail) error
}
