package inbound

import (
	"context"
	"time"

	"github.com/google/uuid"

	"trail-replay/internal/core/trail/domain"
)

type RevertService interface {
	CreateRevertJob(ctx context.Context, from, to time.Time) (*domain.RevertJob, error)
	GetRevertJob(ctx context.Context, id uuid.UUID) (*domain.RevertJob, error)
	ListRevertJobs(ctx context.Context, page, pageSize int) (*domain.PaginatedResponse[domain.RevertJob], error)
	CancelRevertJob(ctx context.Context, id uuid.UUID) (*domain.RevertJob, error)
}
