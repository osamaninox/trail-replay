package outbound

import (
	"context"

	"github.com/google/uuid"

	"trail-replay/internal/core/trail/domain"
)

// SourceDatabaseRepository is the driven port for source database config persistence.
type SourceDatabaseRepository interface {
	Save(ctx context.Context, db *domain.SourceDatabase) error
	FindByID(ctx context.Context, id uuid.UUID) (*domain.SourceDatabase, error)
	FindAllByUser(ctx context.Context, userID uuid.UUID) ([]domain.SourceDatabase, error)
	Update(ctx context.Context, db *domain.SourceDatabase) error
	Delete(ctx context.Context, id uuid.UUID) error
}
