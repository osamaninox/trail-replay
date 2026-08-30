package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"

	"trail-replay/internal/core/trail/domain"
	"trail-replay/internal/core/trail/ports/outbound"
)

const revertAdvisoryLockID = 123456789

type revertRepository struct {
	db *sqlx.DB
}

func NewRevertRepository(db *sqlx.DB) outbound.RevertRepository {
	return &revertRepository{db: db}
}

func (r *revertRepository) CreateJob(ctx context.Context, job *domain.RevertJob) error {
	_, err := r.db.ExecContext(ctx,
		`INSERT INTO revert_job (id, status, input_from, input_to, total_changes, completed_count, failed_count, last_error, created_at, updated_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
		job.ID, string(job.Status), job.InputFrom, job.InputTo,
		job.TotalChanges, job.CompletedCount, job.FailedCount,
		nullableString(job.LastError), job.CreatedAt, job.UpdatedAt,
	)
	return err
}

type revertJobRow struct {
	ID             uuid.UUID  `db:"id"`
	Status         string     `db:"status"`
	InputFrom      time.Time  `db:"input_from"`
	InputTo        time.Time  `db:"input_to"`
	TotalChanges   int        `db:"total_changes"`
	CompletedCount int        `db:"completed_count"`
	FailedCount    int        `db:"failed_count"`
	LastError      *string    `db:"last_error"`
	CreatedAt      time.Time  `db:"created_at"`
	UpdatedAt      time.Time  `db:"updated_at"`
	CompletedAt    *time.Time `db:"completed_at"`
}

func (r *revertRepository) GetJob(ctx context.Context, id uuid.UUID) (*domain.RevertJob, error) {
	var row revertJobRow
	err := r.db.GetContext(ctx, &row,
		`SELECT id, status, input_from, input_to, total_changes, completed_count, failed_count, last_error, created_at, updated_at, completed_at
		 FROM revert_job WHERE id = $1`, id)
	if err != nil {
		if err.Error() == "sql: no rows in result set" {
			return nil, nil
		}
		return nil, err
	}
	return rowToJob(&row), nil
}

func rowToJob(row *revertJobRow) *domain.RevertJob {
	job := &domain.RevertJob{
		ID:             row.ID,
		Status:         domain.RevertJobStatus(row.Status),
		InputFrom:      row.InputFrom,
		InputTo:        row.InputTo,
		TotalChanges:   row.TotalChanges,
		CompletedCount: row.CompletedCount,
		FailedCount:    row.FailedCount,
		CreatedAt:      row.CreatedAt,
		UpdatedAt:      row.UpdatedAt,
		CompletedAt:    row.CompletedAt,
	}
	if row.LastError != nil {
		job.LastError = *row.LastError
	}
	return job
}

func (r *revertRepository) ListJobs(ctx context.Context, offset, limit int) ([]domain.RevertJob, error) {
	var rows []revertJobRow
	err := r.db.SelectContext(ctx, &rows,
		`SELECT id, status, input_from, input_to, total_changes, completed_count, failed_count, last_error, created_at, updated_at, completed_at
		 FROM revert_job ORDER BY created_at DESC LIMIT $1 OFFSET $2`, limit, offset)
	if err != nil {
		return nil, err
	}
	result := make([]domain.RevertJob, len(rows))
	for i, row := range rows {
		result[i] = *rowToJob(&row)
	}
	return result, nil
}

func (r *revertRepository) CountJobs(ctx context.Context) (int, error) {
	var count int
	err := r.db.GetContext(ctx, &count, `SELECT COUNT(*) FROM revert_job`)
	return count, err
}

func (r *revertRepository) UpdateJob(ctx context.Context, job *domain.RevertJob) error {
	_, err := r.db.ExecContext(ctx,
		`UPDATE revert_job SET status = $1, total_changes = $2, completed_count = $3, failed_count = $4, last_error = $5, updated_at = $6, completed_at = $7
		 WHERE id = $8`,
		string(job.Status), job.TotalChanges, job.CompletedCount, job.FailedCount,
		nullableString(job.LastError), job.UpdatedAt, job.CompletedAt, job.ID,
	)
	return err
}

func (r *revertRepository) GetNextPendingJob(ctx context.Context) (*domain.RevertJob, error) {
	var row revertJobRow
	err := r.db.GetContext(ctx, &row,
		`SELECT id, status, input_from, input_to, total_changes, completed_count, failed_count, last_error, created_at, updated_at, completed_at
		 FROM revert_job WHERE status = 'pending' ORDER BY created_at ASC LIMIT 1`)
	if err != nil {
		if err.Error() == "sql: no rows in result set" {
			return nil, nil
		}
		return nil, err
	}
	return rowToJob(&row), nil
}

func (r *revertRepository) GetActiveJob(ctx context.Context) (*domain.RevertJob, error) {
	var row revertJobRow
	err := r.db.GetContext(ctx, &row,
		`SELECT id, status, input_from, input_to, total_changes, completed_count, failed_count, last_error, created_at, updated_at, completed_at
		 FROM revert_job WHERE status = 'in_progress' LIMIT 1`)
	if err != nil {
		if err.Error() == "sql: no rows in result set" {
			return nil, nil
		}
		return nil, err
	}
	return rowToJob(&row), nil
}

func (r *revertRepository) CountUnappliedChanges(ctx context.Context, from, to time.Time) (int, error) {
	var count int
	err := r.db.GetContext(ctx, &count,
		`SELECT COUNT(*) FROM wal_change wc
		 JOIN wal_transaction wt ON wc.transaction_id = wt.id
		 WHERE wc.undo_status = 'not_applied' AND wt.commit_ts >= $1 AND wt.commit_ts <= $2`,
		from, to,
	)
	return count, err
}

func (r *revertRepository) FetchUnappliedChanges(ctx context.Context, from, to time.Time, offset, limit int) ([]domain.WalChangeResult, error) {
	var rows []struct {
		ID             uint64         `db:"id"`
		TransactionID  uint64         `db:"transaction_id"`
		ChangeSeqInTxn int32          `db:"change_seq_in_txn"`
		SchemaName     string         `db:"schema_name"`
		TableName      string         `db:"table_name"`
		Op             string         `db:"op"`
		ChangedColumns pq.StringArray `db:"changed_columns"`
		ForwardDMLSQL  string         `db:"forward_dml_sql"`
		ReverseDMLSQL  string         `db:"reverse_dml_sql"`
		UndoStatus     string         `db:"undo_status"`
		CreatedAt      time.Time      `db:"created_at"`
	}
	err := r.db.SelectContext(ctx, &rows,
		`SELECT wc.id, wc.transaction_id, wc.change_seq_in_txn, wc.schema_name, wc.table_name, wc.op,
		        wc.changed_columns, wc.forward_dml_sql, wc.reverse_dml_sql, wc.undo_status, wc.created_at
		 FROM wal_change wc
		 JOIN wal_transaction wt ON wc.transaction_id = wt.id
		 WHERE wc.undo_status = 'not_applied' AND wt.commit_ts >= $1 AND wt.commit_ts <= $2
		 ORDER BY wt.commit_lsn DESC, wc.change_seq_in_txn DESC
		 LIMIT $3 OFFSET $4`,
		from, to, limit, offset,
	)
	if err != nil {
		return nil, fmt.Errorf("fetch unapplied changes: %w", err)
	}
	result := make([]domain.WalChangeResult, len(rows))
	for i, row := range rows {
		result[i] = domain.WalChangeResult{
			ID:             row.ID,
			TransactionID:  row.TransactionID,
			ChangeSeqInTxn: row.ChangeSeqInTxn,
			SchemaName:     row.SchemaName,
			TableName:      row.TableName,
			Op:             row.Op,
			ChangedColumns: []string(row.ChangedColumns),
			ForwardDMLSQL:  row.ForwardDMLSQL,
			ReverseDMLSQL:  row.ReverseDMLSQL,
			UndoStatus:     row.UndoStatus,
			CreatedAt:      row.CreatedAt,
		}
	}
	return result, nil
}

func (r *revertRepository) MarkChangeApplied(ctx context.Context, jobID uuid.UUID, change domain.WalChangeResult) error {
	tx, err := r.db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	_, err = tx.ExecContext(ctx,
		`UPDATE wal_change SET undo_status = 'applied', undo_applied_at = now(), undo_applied_by = $1 WHERE id = $2`,
		jobID.String(), change.ID,
	)
	if err != nil {
		return fmt.Errorf("update wal_change: %w", err)
	}

	_, err = tx.ExecContext(ctx,
		`INSERT INTO revert_job_change (job_id, change_id, status, applied_at)
		 VALUES ($1, $2, 'applied', now())`,
		jobID, change.ID,
	)
	if err != nil {
		return fmt.Errorf("insert revert_job_change: %w", err)
	}

	return tx.Commit()
}

func (r *revertRepository) MarkChangeFailed(ctx context.Context, jobID uuid.UUID, change domain.WalChangeResult, errMsg string) error {
	_, err := r.db.ExecContext(ctx,
		`INSERT INTO revert_job_change (job_id, change_id, status, error_message)
		 VALUES ($1, $2, 'failed', $3)`,
		jobID, change.ID, errMsg,
	)
	return err
}

func (r *revertRepository) TryAcquireAdvisoryLock(ctx context.Context) (bool, error) {
	var locked bool
	err := r.db.GetContext(ctx, &locked, "SELECT pg_try_advisory_lock($1)", revertAdvisoryLockID)
	return locked, err
}

func (r *revertRepository) ReleaseAdvisoryLock(ctx context.Context) error {
	_, err := r.db.ExecContext(ctx, "SELECT pg_advisory_unlock($1)", revertAdvisoryLockID)
	return err
}

func nullableString(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
