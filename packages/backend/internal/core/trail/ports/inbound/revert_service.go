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
	ListJobTransactions(ctx context.Context, jobID uuid.UUID, page, pageSize int) (*domain.PaginatedResponse[domain.RevertJobTransaction], error)
	CancelRevertJob(ctx context.Context, id uuid.UUID) (*domain.RevertJob, error)

	// PreviewRevert dry-runs the revert of [from, to] on the source database
	// (rolled back, nothing applied) and returns each affected row's current
	// and post-revert state.
	PreviewRevert(ctx context.Context, from, to time.Time) (*domain.RevertPreview, error)
}
