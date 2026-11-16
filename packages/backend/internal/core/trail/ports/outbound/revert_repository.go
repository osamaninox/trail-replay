package outbound

import (
	"context"
	"time"

	"github.com/google/uuid"

	"trail-replay/internal/core/trail/domain"
)

// SQLTx is the minimal transaction surface the repository needs when a caller
// manages the transaction boundary (e.g. a DBOS durable transaction step).
// It is implemented by adapters wrapping driver transactions.
type SQLTx interface {
	Exec(ctx context.Context, query string, args ...any) error
}

type RevertRepository interface {
	CreateJob(ctx context.Context, job *domain.RevertJob) error
	GetJob(ctx context.Context, id uuid.UUID) (*domain.RevertJob, error)
	ListJobs(ctx context.Context, offset, limit int) ([]domain.RevertJob, error)
	CountJobs(ctx context.Context) (int, error)
	UpdateJob(ctx context.Context, job *domain.RevertJob) error

	// CountRevertableTransactions counts transactions in [from, to] that no job
	// has applied yet (anti-join on revert_job_transaction).
	CountRevertableTransactions(ctx context.Context, from, to time.Time) (int, error)

	// ListRevertableChanges returns the individual changes a revert of
	// [from, to] would undo, in revert execution order (commit_lsn DESC,
	// change_seq_in_txn DESC), with the same already-applied exclusion as
	// SnapshotPlan. Used to build revert previews.
	ListRevertableChanges(ctx context.Context, from, to time.Time) ([]domain.RevertChange, error)

	// SnapshotPlan freezes the ordered work list for a job (commit_lsn DESC) and
	// returns the number of transactions snapshotted and their total change count.
	// Idempotent: re-running for the same job is a no-op.
	SnapshotPlan(ctx context.Context, jobID uuid.UUID, from, to time.Time) (count, totalChanges int, err error)

	GetJobTransaction(ctx context.Context, jobID uuid.UUID, seq int) (*domain.RevertJobTransaction, error)
	ListJobTransactions(ctx context.Context, jobID uuid.UUID, offset, limit int) ([]domain.RevertJobTransaction, error)
	CountJobTransactions(ctx context.Context, jobID uuid.UUID) (int, error)
	GetTransactionChanges(ctx context.Context, transactionID uint64) ([]domain.WalChangeResult, error)

	// MarkTransactionApplied/Failed update the snapshot row and increment the
	// job counter by that row's change_count, in one statement. The tx arg
	// joins a caller-managed transaction (e.g. a DBOS durable transaction step)
	// so mark + step record commit atomically.
	MarkTransactionApplied(ctx context.Context, tx SQLTx, jobID uuid.UUID, seq int) error
	MarkTransactionFailed(ctx context.Context, tx SQLTx, jobID uuid.UUID, seq int, errMsg string) error

	// ListPendingJobIDs returns IDs of jobs never submitted to the workflow
	// engine (startup reconciliation).
	ListPendingJobIDs(ctx context.Context) ([]uuid.UUID, error)
}
