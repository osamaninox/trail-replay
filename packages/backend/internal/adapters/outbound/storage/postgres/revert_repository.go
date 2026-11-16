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

// CountRevertableTransactions estimates job size at creation time. Eligibility
// is an anti-join against revert_job_transaction (wal_change.undo_* columns are
// legacy/unused), not undo_status='not_applied'.
func (r *revertRepository) CountRevertableTransactions(ctx context.Context, from, to time.Time) (int, error) {
	var count int
	err := r.db.GetContext(ctx, &count,
		`SELECT COUNT(*) FROM wal_transaction wt
		 WHERE wt.commit_ts >= $1 AND wt.commit_ts <= $2
		   AND NOT EXISTS (
		       SELECT 1 FROM revert_job_transaction rjt
		       WHERE rjt.transaction_id = wt.id AND rjt.status = 'applied'
		   )`,
		from, to,
	)
	return count, err
}

// ListRevertableChanges returns the window's changes in revert execution
// order with the row payloads a preview needs. Same eligibility anti-join as
// SnapshotPlan: changes whose transaction was already applied by a job are
// excluded.
func (r *revertRepository) ListRevertableChanges(ctx context.Context, from, to time.Time) ([]domain.RevertChange, error) {
	var rows []struct {
		ID             uint64         `db:"id"`
		TransactionID  uint64         `db:"transaction_id"`
		ChangeSeqInTxn int32          `db:"change_seq_in_txn"`
		SchemaName     string         `db:"schema_name"`
		TableName      string         `db:"table_name"`
		Op             string         `db:"op"`
		ChangedColumns pq.StringArray `db:"changed_columns"`
		OldRow         *PayloadJSON   `db:"old_row"`
		NewRow         *PayloadJSON   `db:"new_row"`
		ReverseDMLSQL  string         `db:"reverse_dml_sql"`
	}
	err := r.db.SelectContext(ctx, &rows,
		`SELECT c.id, c.transaction_id, c.change_seq_in_txn, c.schema_name, c.table_name, c.op,
		        c.changed_columns, c.old_row, c.new_row, c.reverse_dml_sql
		 FROM wal_change c
		 JOIN wal_transaction wt ON wt.id = c.transaction_id
		 WHERE wt.commit_ts >= $1 AND wt.commit_ts <= $2
		   AND NOT EXISTS (
		       SELECT 1 FROM revert_job_transaction rjt
		       WHERE rjt.transaction_id = wt.id AND rjt.status = 'applied'
		   )
		 ORDER BY wt.commit_lsn DESC, c.change_seq_in_txn DESC`,
		from, to,
	)
	if err != nil {
		return nil, fmt.Errorf("list revertable changes: %w", err)
	}
	result := make([]domain.RevertChange, len(rows))
	for i, row := range rows {
		result[i] = domain.RevertChange{
			ID:             row.ID,
			TransactionID:  row.TransactionID,
			ChangeSeqInTxn: row.ChangeSeqInTxn,
			SchemaName:     row.SchemaName,
			TableName:      row.TableName,
			Op:             row.Op,
			ChangedColumns: []string(row.ChangedColumns),
			OldRow:         payloadToMap(row.OldRow),
			NewRow:         payloadToMap(row.NewRow),
			ReverseDMLSQL:  row.ReverseDMLSQL,
		}
	}
	return result, nil
}

func payloadToMap(p *PayloadJSON) map[string]any {
	if p == nil || *p == nil {
		return nil
	}
	return map[string]any(*p)
}

// SnapshotPlan freezes the work list once: seq is assigned by row_number() over
// commit_lsn DESC so "step i" always means the same transaction across
// crash/replay. ON CONFLICT DO NOTHING makes replay after a lost checkpoint a
// no-op; the count/sum then read from the snapshot itself.
func (r *revertRepository) SnapshotPlan(ctx context.Context, jobID uuid.UUID, from, to time.Time) (count, totalChanges int, err error) {
	_, err = r.db.ExecContext(ctx,
		`INSERT INTO revert_job_transaction (job_id, seq, transaction_id, commit_lsn, change_count)
		 SELECT $1, row_number() OVER (ORDER BY wt.commit_lsn DESC), wt.id, wt.commit_lsn, wt.change_count
		 FROM wal_transaction wt
		 WHERE wt.commit_ts >= $2 AND wt.commit_ts <= $3
		   AND NOT EXISTS (
		       SELECT 1 FROM revert_job_transaction rjt
		       WHERE rjt.transaction_id = wt.id AND rjt.status = 'applied'
		   )
		 ON CONFLICT DO NOTHING`,
		jobID, from, to,
	)
	if err != nil {
		return 0, 0, fmt.Errorf("snapshot plan: %w", err)
	}

	err = r.db.QueryRowContext(ctx,
		`SELECT COUNT(*), COALESCE(SUM(change_count), 0)
		 FROM revert_job_transaction WHERE job_id = $1`,
		jobID,
	).Scan(&count, &totalChanges)
	if err != nil {
		return 0, 0, fmt.Errorf("count snapshot: %w", err)
	}
	return count, totalChanges, nil
}

type revertJobTransactionRow struct {
	JobID         uuid.UUID  `db:"job_id"`
	Seq           int        `db:"seq"`
	TransactionID uint64     `db:"transaction_id"`
	CommitLSN     string     `db:"commit_lsn"`
	ChangeCount   int        `db:"change_count"`
	Status        string     `db:"status"`
	ErrorMessage  *string    `db:"error_message"`
	AppliedAt     *time.Time `db:"applied_at"`
}

func rowToJobTransaction(row *revertJobTransactionRow) *domain.RevertJobTransaction {
	t := &domain.RevertJobTransaction{
		JobID:         row.JobID,
		Seq:           row.Seq,
		TransactionID: row.TransactionID,
		CommitLSN:     row.CommitLSN,
		ChangeCount:   row.ChangeCount,
		Status:        domain.RevertTransactionStatus(row.Status),
		AppliedAt:     row.AppliedAt,
	}
	if row.ErrorMessage != nil {
		t.ErrorMessage = *row.ErrorMessage
	}
	return t
}

const jobTransactionColumns = `job_id, seq, transaction_id, commit_lsn::text AS commit_lsn, change_count, status, error_message, applied_at`

func (r *revertRepository) GetJobTransaction(ctx context.Context, jobID uuid.UUID, seq int) (*domain.RevertJobTransaction, error) {
	var row revertJobTransactionRow
	err := r.db.GetContext(ctx, &row,
		`SELECT `+jobTransactionColumns+` FROM revert_job_transaction WHERE job_id = $1 AND seq = $2`, jobID, seq)
	if err != nil {
		if err.Error() == "sql: no rows in result set" {
			return nil, nil
		}
		return nil, err
	}
	return rowToJobTransaction(&row), nil
}

func (r *revertRepository) ListJobTransactions(ctx context.Context, jobID uuid.UUID, offset, limit int) ([]domain.RevertJobTransaction, error) {
	var rows []revertJobTransactionRow
	err := r.db.SelectContext(ctx, &rows,
		`SELECT `+jobTransactionColumns+` FROM revert_job_transaction WHERE job_id = $1 ORDER BY seq LIMIT $2 OFFSET $3`,
		jobID, limit, offset)
	if err != nil {
		return nil, err
	}
	result := make([]domain.RevertJobTransaction, len(rows))
	for i, row := range rows {
		result[i] = *rowToJobTransaction(&row)
	}
	return result, nil
}

func (r *revertRepository) CountJobTransactions(ctx context.Context, jobID uuid.UUID) (int, error) {
	var count int
	err := r.db.GetContext(ctx, &count,
		`SELECT COUNT(*) FROM revert_job_transaction WHERE job_id = $1`, jobID)
	return count, err
}

// GetTransactionChanges returns one transaction's changes in revert order
// (change_seq_in_txn DESC), scoped to a single txn from the frozen snapshot.
func (r *revertRepository) GetTransactionChanges(ctx context.Context, transactionID uint64) ([]domain.WalChangeResult, error) {
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
		`SELECT id, transaction_id, change_seq_in_txn, schema_name, table_name, op,
		        changed_columns, forward_dml_sql, reverse_dml_sql, undo_status, created_at
		 FROM wal_change WHERE transaction_id = $1
		 ORDER BY change_seq_in_txn DESC`,
		transactionID,
	)
	if err != nil {
		return nil, fmt.Errorf("fetch transaction changes: %w", err)
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

// MarkTransactionApplied/Failed run on the caller's SQLTx (a DBOS durable
// transaction): snapshot-row update + job counter increment by that row's
// change_count, as one CTE statement — no read-then-mark gap. The surrounding
// transaction also carries DBOS's step record, so the mark commits atomically
// with it (structural exactly-once, plan §7 decision B).
func (r *revertRepository) MarkTransactionApplied(ctx context.Context, tx outbound.SQLTx, jobID uuid.UUID, seq int) error {
	return tx.Exec(ctx,
		`WITH upd AS (
		     UPDATE revert_job_transaction
		     SET status = 'applied', applied_at = now(), error_message = NULL
		     WHERE job_id = $1 AND seq = $2
		     RETURNING change_count
		 )
		 UPDATE revert_job
		 SET completed_count = completed_count + (SELECT change_count FROM upd), updated_at = now()
		 WHERE id = $1`,
		jobID, seq,
	)
}

func (r *revertRepository) MarkTransactionFailed(ctx context.Context, tx outbound.SQLTx, jobID uuid.UUID, seq int, errMsg string) error {
	return tx.Exec(ctx,
		`WITH upd AS (
		     UPDATE revert_job_transaction
		     SET status = 'failed', error_message = $3
		     WHERE job_id = $1 AND seq = $2
		     RETURNING change_count
		 )
		 UPDATE revert_job
		 SET failed_count = failed_count + (SELECT change_count FROM upd), updated_at = now()
		 WHERE id = $1`,
		jobID, seq, errMsg,
	)
}

// ListPendingJobIDs feeds startup reconciliation: jobs whose insert committed
// but whose DBOS enqueue was lost get resubmitted on boot.
func (r *revertRepository) ListPendingJobIDs(ctx context.Context) ([]uuid.UUID, error) {
	var ids []uuid.UUID
	err := r.db.SelectContext(ctx, &ids,
		`SELECT id FROM revert_job WHERE status = 'pending' ORDER BY created_at ASC`)
	if err != nil {
		return nil, err
	}
	return ids, nil
}

func nullableString(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
