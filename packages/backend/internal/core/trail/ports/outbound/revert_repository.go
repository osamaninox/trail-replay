package outbound

import (
	"context"
	"time"

	"github.com/google/uuid"

	"trail-replay/internal/core/trail/domain"
)

type RevertRepository interface {
	CreateJob(ctx context.Context, job *domain.RevertJob) error
	GetJob(ctx context.Context, id uuid.UUID) (*domain.RevertJob, error)
	ListJobs(ctx context.Context, offset, limit int) ([]domain.RevertJob, error)
	CountJobs(ctx context.Context) (int, error)
	UpdateJob(ctx context.Context, job *domain.RevertJob) error

	GetNextPendingJob(ctx context.Context) (*domain.RevertJob, error)
	GetActiveJob(ctx context.Context) (*domain.RevertJob, error)
	CountUnappliedChanges(ctx context.Context, from, to time.Time) (int, error)
	FetchUnappliedChanges(ctx context.Context, from, to time.Time, offset, limit int) ([]domain.WalChangeResult, error)
	MarkChangeApplied(ctx context.Context, jobID uuid.UUID, change domain.WalChangeResult) error
	MarkChangeFailed(ctx context.Context, jobID uuid.UUID, change domain.WalChangeResult, errMsg string) error

	TryAcquireAdvisoryLock(ctx context.Context) (bool, error)
	ReleaseAdvisoryLock(ctx context.Context) error
}
