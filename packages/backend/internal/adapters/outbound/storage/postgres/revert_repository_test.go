package postgres_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	"github.com/pressly/goose/v3"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"trail-replay/internal/adapters/outbound/storage/postgres"
	"trail-replay/internal/core/trail/domain"
	"trail-replay/internal/core/trail/ports/outbound"
)

func skipShortCI(t *testing.T) {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	if os.Getenv("CI") != "" && os.Getenv("SKIP_INTEGRATION") != "" {
		t.Skip("skipping integration test in CI")
	}
}

func startPGWithRevertTables(ctx context.Context, t *testing.T) (*tcpostgres.PostgresContainer, *sqlx.DB) {
	t.Helper()
	skipShortCI(t)

	pgContainer, err := tcpostgres.Run(ctx,
		"postgres:16-alpine",
		tcpostgres.WithDatabase("trailwal"),
		tcpostgres.WithUsername("testuser"),
		tcpostgres.WithPassword("testpass"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(60*time.Second),
		),
	)
	if err != nil {
		t.Fatalf("failed to start postgres container: %v", err)
	}

	dsn, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("failed to get connection string: %v", err)
	}

	db, err := sqlx.Connect("postgres", dsn)
	if err != nil {
		t.Fatalf("failed to connect to postgres: %v", err)
	}

	if err := runRevertMigrations(db); err != nil {
		t.Fatalf("failed to run migrations: %v", err)
	}

	return pgContainer, db
}

// runRevertMigrations applies the real goose migrations so tests always run
// against the shipped schema (go test runs with cwd = package dir).
func runRevertMigrations(db *sqlx.DB) error {
	if err := goose.SetDialect("postgres"); err != nil {
		return err
	}
	return goose.Up(db.DB, "../../../../../migrations")
}

func insertTestTxn(t *testing.T, db *sqlx.DB, id uint64, commitTS time.Time, changeCount int) {
	t.Helper()
	lsn := fmt.Sprintf("0/%X", id)
	_, err := db.Exec(
		`INSERT INTO wal_transaction (id, source_slot, source_db, xid, commit_lsn, commit_ts, change_count, ingested_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		id, "test_slot", "testdb", int64(id*100), lsn, commitTS, changeCount, commitTS,
	)
	if err != nil {
		t.Fatalf("insert txn %d: %v", id, err)
	}
}

func insertTestChange(t *testing.T, db *sqlx.DB, id, txnID uint64, seq int32, tableName, op, reverseSQL string) {
	t.Helper()
	_, err := db.Exec(
		`INSERT INTO wal_change (id, transaction_id, change_seq_in_txn, schema_name, table_name, op, forward_dml_sql, reverse_dml_sql)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		id, txnID, seq, "public", tableName, op, reverseSQL, reverseSQL,
	)
	if err != nil {
		t.Fatalf("insert change %d: %v", id, err)
	}
}

func createTestJob(t *testing.T, repo outbound.RevertRepository) *domain.RevertJob {
	t.Helper()
	ctx := context.Background()
	from := time.Now().Add(-1 * time.Hour).UTC().Truncate(time.Microsecond)
	to := time.Now().UTC().Truncate(time.Microsecond)
	job := domain.NewRevertJob(from, to)
	if err := repo.CreateJob(ctx, job); err != nil {
		t.Fatalf("create job: %v", err)
	}
	return job
}

// testSQLTx wraps a driver transaction so mark methods can join a
// caller-managed transaction, as the DBOS durable-transaction step does.
type testSQLTx struct{ tx *sqlx.Tx }

func (t testSQLTx) Exec(ctx context.Context, query string, args ...any) error {
	_, err := t.tx.ExecContext(ctx, query, args...)
	return err
}

func beginTestTx(ctx context.Context, t *testing.T, db *sqlx.DB) (outbound.SQLTx, *sqlx.Tx) {
	t.Helper()
	tx, err := db.BeginTxx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	return testSQLTx{tx}, tx
}

func TestCreateAndGetJob(t *testing.T) {
	ctx := context.Background()
	pgContainer, db := startPGWithRevertTables(ctx, t)
	defer func() { pgContainer.Terminate(ctx) }()
	defer db.Close()

	repo := postgres.NewRevertRepository(db)

	from := time.Now().Add(-1 * time.Hour).UTC().Truncate(time.Microsecond)
	to := time.Now().UTC().Truncate(time.Microsecond)
	job := domain.NewRevertJob(from, to)
	job.TotalChanges = 5

	if err := repo.CreateJob(ctx, job); err != nil {
		t.Fatalf("create job: %v", err)
	}

	got, err := repo.GetJob(ctx, job.ID)
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	if got == nil {
		t.Fatal("expected job, got nil")
	}
	if got.ID != job.ID {
		t.Errorf("id mismatch")
	}
	if got.Status != domain.RevertJobStatusPending {
		t.Errorf("expected pending, got %s", got.Status)
	}
	if got.TotalChanges != 5 {
		t.Errorf("expected TotalChanges=5, got %d", got.TotalChanges)
	}
}

func TestGetJob_NotFound(t *testing.T) {
	ctx := context.Background()
	pgContainer, db := startPGWithRevertTables(ctx, t)
	defer func() { pgContainer.Terminate(ctx) }()
	defer db.Close()

	repo := postgres.NewRevertRepository(db)

	got, err := repo.GetJob(ctx, uuid.New())
	if err != nil {
		t.Fatalf("get job error: %v", err)
	}
	if got != nil {
		t.Error("expected nil for non-existent job")
	}
}

func TestListJobs(t *testing.T) {
	ctx := context.Background()
	pgContainer, db := startPGWithRevertTables(ctx, t)
	defer func() { pgContainer.Terminate(ctx) }()
	defer db.Close()

	repo := postgres.NewRevertRepository(db)

	from := time.Now().Add(-2 * time.Hour).UTC().Truncate(time.Microsecond)
	to := time.Now().UTC().Truncate(time.Microsecond)

	job1 := domain.NewRevertJob(from, to)
	repo.CreateJob(ctx, job1)

	job2 := domain.NewRevertJob(from, to)
	repo.CreateJob(ctx, job2)

	jobs, err := repo.ListJobs(ctx, 0, 10)
	if err != nil {
		t.Fatalf("list jobs: %v", err)
	}
	if len(jobs) != 2 {
		t.Errorf("expected 2 jobs, got %d", len(jobs))
	}
}

func TestListJobs_Pagination(t *testing.T) {
	ctx := context.Background()
	pgContainer, db := startPGWithRevertTables(ctx, t)
	defer func() { pgContainer.Terminate(ctx) }()
	defer db.Close()

	repo := postgres.NewRevertRepository(db)

	from := time.Now().Add(-1 * time.Hour).UTC().Truncate(time.Microsecond)
	to := time.Now().UTC().Truncate(time.Microsecond)

	for i := 0; i < 5; i++ {
		job := domain.NewRevertJob(from, to)
		repo.CreateJob(ctx, job)
	}

	page1, err := repo.ListJobs(ctx, 0, 2)
	if err != nil {
		t.Fatalf("list page1: %v", err)
	}
	if len(page1) != 2 {
		t.Fatalf("expected 2 on page 1, got %d", len(page1))
	}

	page3, err := repo.ListJobs(ctx, 4, 2)
	if err != nil {
		t.Fatalf("list page3: %v", err)
	}
	if len(page3) != 1 {
		t.Fatalf("expected 1 on page 3, got %d", len(page3))
	}
}

func TestCountJobs(t *testing.T) {
	ctx := context.Background()
	pgContainer, db := startPGWithRevertTables(ctx, t)
	defer func() { pgContainer.Terminate(ctx) }()
	defer db.Close()

	repo := postgres.NewRevertRepository(db)

	from := time.Now().Add(-1 * time.Hour).UTC().Truncate(time.Microsecond)
	to := time.Now().UTC().Truncate(time.Microsecond)

	for i := 0; i < 3; i++ {
		job := domain.NewRevertJob(from, to)
		repo.CreateJob(ctx, job)
	}

	count, err := repo.CountJobs(ctx)
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 3 {
		t.Errorf("expected 3, got %d", count)
	}
}

func TestUpdateJob(t *testing.T) {
	ctx := context.Background()
	pgContainer, db := startPGWithRevertTables(ctx, t)
	defer func() { pgContainer.Terminate(ctx) }()
	defer db.Close()

	repo := postgres.NewRevertRepository(db)

	from := time.Now().Add(-1 * time.Hour).UTC().Truncate(time.Microsecond)
	to := time.Now().UTC().Truncate(time.Microsecond)
	job := domain.NewRevertJob(from, to)
	repo.CreateJob(ctx, job)

	job.Status = domain.RevertJobStatusInProgress
	job.CompletedCount = 10
	if err := repo.UpdateJob(ctx, job); err != nil {
		t.Fatalf("update job: %v", err)
	}

	got, _ := repo.GetJob(ctx, job.ID)
	if got.Status != domain.RevertJobStatusInProgress {
		t.Errorf("expected in_progress, got %s", got.Status)
	}
	if got.CompletedCount != 10 {
		t.Errorf("expected CompletedCount=10, got %d", got.CompletedCount)
	}
}

func TestCountRevertableTransactions(t *testing.T) {
	ctx := context.Background()
	pgContainer, db := startPGWithRevertTables(ctx, t)
	defer func() { pgContainer.Terminate(ctx) }()
	defer db.Close()

	repo := postgres.NewRevertRepository(db)

	now := time.Now().UTC().Truncate(time.Microsecond)
	insertTestTxn(t, db, 1, now.Add(-30*time.Minute), 1)
	insertTestTxn(t, db, 2, now.Add(-20*time.Minute), 1)
	insertTestTxn(t, db, 3, now.Add(-5*time.Minute), 1)
	insertTestTxn(t, db, 4, now.Add(1*time.Hour), 1) // outside window

	// Txn 2 already reverted by another job → excluded by the anti-join.
	otherJob := createTestJob(t, repo)
	_, err := db.Exec(
		`INSERT INTO revert_job_transaction (job_id, seq, transaction_id, commit_lsn, change_count, status)
		 VALUES ($1, 1, 2, '0/2', 1, 'applied')`, otherJob.ID)
	if err != nil {
		t.Fatalf("insert applied txn: %v", err)
	}

	count, err := repo.CountRevertableTransactions(ctx, now.Add(-1*time.Hour), now)
	if err != nil {
		t.Fatalf("count revertable: %v", err)
	}
	if count != 2 {
		t.Errorf("expected 2 revertable transactions (1,3), got %d", count)
	}
}

func TestSnapshotPlan(t *testing.T) {
	ctx := context.Background()
	pgContainer, db := startPGWithRevertTables(ctx, t)
	defer func() { pgContainer.Terminate(ctx) }()
	defer db.Close()

	repo := postgres.NewRevertRepository(db)

	now := time.Now().UTC().Truncate(time.Microsecond)
	insertTestTxn(t, db, 1, now.Add(-30*time.Minute), 2)
	insertTestTxn(t, db, 2, now.Add(-20*time.Minute), 3)
	insertTestTxn(t, db, 3, now.Add(-5*time.Minute), 5)

	job := createTestJob(t, repo)

	count, totalChanges, err := repo.SnapshotPlan(ctx, job.ID, now.Add(-1*time.Hour), now)
	if err != nil {
		t.Fatalf("snapshot plan: %v", err)
	}
	if count != 3 {
		t.Errorf("expected count=3, got %d", count)
	}
	if totalChanges != 10 {
		t.Errorf("expected totalChanges=10, got %d", totalChanges)
	}

	// seq assigned by commit_lsn DESC: newest transaction first.
	txns, err := repo.ListJobTransactions(ctx, job.ID, 0, 10)
	if err != nil {
		t.Fatalf("list job transactions: %v", err)
	}
	wantOrder := []uint64{3, 2, 1}
	if len(txns) != len(wantOrder) {
		t.Fatalf("expected %d snapshot rows, got %d", len(wantOrder), len(txns))
	}
	for i, wantTxnID := range wantOrder {
		if txns[i].Seq != i+1 {
			t.Errorf("row %d: expected seq=%d, got %d", i, i+1, txns[i].Seq)
		}
		if txns[i].TransactionID != wantTxnID {
			t.Errorf("row %d: expected transaction_id=%d, got %d", i, wantTxnID, txns[i].TransactionID)
		}
		if txns[i].Status != domain.RevertTransactionStatusPending {
			t.Errorf("row %d: expected pending, got %s", i, txns[i].Status)
		}
	}
	if txns[0].CommitLSN != "0/3" {
		t.Errorf("expected commit_lsn 0/3, got %s", txns[0].CommitLSN)
	}
}

func TestSnapshotPlan_Idempotent(t *testing.T) {
	ctx := context.Background()
	pgContainer, db := startPGWithRevertTables(ctx, t)
	defer func() { pgContainer.Terminate(ctx) }()
	defer db.Close()

	repo := postgres.NewRevertRepository(db)

	now := time.Now().UTC().Truncate(time.Microsecond)
	insertTestTxn(t, db, 1, now.Add(-30*time.Minute), 2)
	insertTestTxn(t, db, 2, now.Add(-20*time.Minute), 3)

	job := createTestJob(t, repo)

	count1, total1, err := repo.SnapshotPlan(ctx, job.ID, now.Add(-1*time.Hour), now)
	if err != nil {
		t.Fatalf("first snapshot: %v", err)
	}

	// Replay after a lost checkpoint: same job re-plans → no-op, same counts.
	count2, total2, err := repo.SnapshotPlan(ctx, job.ID, now.Add(-1*time.Hour), now)
	if err != nil {
		t.Fatalf("second snapshot: %v", err)
	}
	if count1 != count2 || total1 != total2 {
		t.Errorf("expected identical counts, got (%d,%d) then (%d,%d)", count1, total1, count2, total2)
	}

	var rowCount int
	if err := db.Get(&rowCount, `SELECT COUNT(*) FROM revert_job_transaction WHERE job_id = $1`, job.ID); err != nil {
		t.Fatalf("count rows: %v", err)
	}
	if rowCount != 2 {
		t.Errorf("expected 2 snapshot rows (no duplicates), got %d", rowCount)
	}
}

func TestSnapshotPlan_ExcludesAppliedByOtherJob(t *testing.T) {
	ctx := context.Background()
	pgContainer, db := startPGWithRevertTables(ctx, t)
	defer func() { pgContainer.Terminate(ctx) }()
	defer db.Close()

	repo := postgres.NewRevertRepository(db)

	now := time.Now().UTC().Truncate(time.Microsecond)
	insertTestTxn(t, db, 1, now.Add(-30*time.Minute), 2)
	insertTestTxn(t, db, 2, now.Add(-20*time.Minute), 3)

	jobA := createTestJob(t, repo)
	if _, _, err := repo.SnapshotPlan(ctx, jobA.ID, now.Add(-1*time.Hour), now); err != nil {
		t.Fatalf("snapshot A: %v", err)
	}
	// Job A applied the txn at seq 1 (txn 2, highest LSN).
	if _, err := db.Exec(
		`UPDATE revert_job_transaction SET status = 'applied', applied_at = now() WHERE job_id = $1 AND seq = 1`,
		jobA.ID); err != nil {
		t.Fatalf("mark applied for A: %v", err)
	}

	jobB := createTestJob(t, repo)
	count, totalChanges, err := repo.SnapshotPlan(ctx, jobB.ID, now.Add(-1*time.Hour), now)
	if err != nil {
		t.Fatalf("snapshot B: %v", err)
	}
	if count != 1 || totalChanges != 2 {
		t.Errorf("expected B to snapshot only txn 1 (count=1,total=2), got (%d,%d)", count, totalChanges)
	}
}

func TestJobTransactionQueries(t *testing.T) {
	ctx := context.Background()
	pgContainer, db := startPGWithRevertTables(ctx, t)
	defer func() { pgContainer.Terminate(ctx) }()
	defer db.Close()

	repo := postgres.NewRevertRepository(db)

	now := time.Now().UTC().Truncate(time.Microsecond)
	insertTestTxn(t, db, 1, now.Add(-30*time.Minute), 2)
	insertTestTxn(t, db, 2, now.Add(-20*time.Minute), 3)
	insertTestTxn(t, db, 3, now.Add(-5*time.Minute), 5)

	job := createTestJob(t, repo)
	if _, _, err := repo.SnapshotPlan(ctx, job.ID, now.Add(-1*time.Hour), now); err != nil {
		t.Fatalf("snapshot: %v", err)
	}

	got, err := repo.GetJobTransaction(ctx, job.ID, 2)
	if err != nil {
		t.Fatalf("get job transaction: %v", err)
	}
	if got == nil {
		t.Fatal("expected job transaction at seq 2")
	}
	if got.TransactionID != 2 || got.CommitLSN != "0/2" || got.ChangeCount != 3 {
		t.Errorf("unexpected row: %+v", got)
	}

	page, err := repo.ListJobTransactions(ctx, job.ID, 1, 1)
	if err != nil {
		t.Fatalf("list page: %v", err)
	}
	if len(page) != 1 || page[0].Seq != 2 {
		t.Errorf("expected [seq 2], got %+v", page)
	}

	count, err := repo.CountJobTransactions(ctx, job.ID)
	if err != nil {
		t.Fatalf("count job transactions: %v", err)
	}
	if count != 3 {
		t.Errorf("expected 3, got %d", count)
	}

	missing, err := repo.GetJobTransaction(ctx, job.ID, 99)
	if err != nil {
		t.Fatalf("get missing: %v", err)
	}
	if missing != nil {
		t.Error("expected nil for missing seq")
	}
}

func TestGetTransactionChanges(t *testing.T) {
	ctx := context.Background()
	pgContainer, db := startPGWithRevertTables(ctx, t)
	defer func() { pgContainer.Terminate(ctx) }()
	defer db.Close()

	repo := postgres.NewRevertRepository(db)

	now := time.Now().UTC().Truncate(time.Microsecond)
	insertTestTxn(t, db, 1, now.Add(-10*time.Minute), 3)
	insertTestChange(t, db, 1, 1, 1, "users", "U", "UPDATE users SET email='a' WHERE id=1")
	insertTestChange(t, db, 2, 1, 2, "orders", "D", "INSERT INTO orders (id) VALUES (9)")
	insertTestChange(t, db, 3, 1, 3, "users", "I", "DELETE FROM users WHERE id=2")

	changes, err := repo.GetTransactionChanges(ctx, 1)
	if err != nil {
		t.Fatalf("get transaction changes: %v", err)
	}
	if len(changes) != 3 {
		t.Fatalf("expected 3 changes, got %d", len(changes))
	}
	// Revert order: change_seq_in_txn DESC.
	for i, wantSeq := range []int32{3, 2, 1} {
		if changes[i].ChangeSeqInTxn != wantSeq {
			t.Errorf("change %d: expected seq %d, got %d", i, wantSeq, changes[i].ChangeSeqInTxn)
		}
	}
	if changes[0].ReverseDMLSQL != "DELETE FROM users WHERE id=2" {
		t.Errorf("unexpected reverse SQL: %s", changes[0].ReverseDMLSQL)
	}
}

func TestMarkTransactionApplied(t *testing.T) {
	ctx := context.Background()
	pgContainer, db := startPGWithRevertTables(ctx, t)
	defer func() { pgContainer.Terminate(ctx) }()
	defer db.Close()

	repo := postgres.NewRevertRepository(db)

	now := time.Now().UTC().Truncate(time.Microsecond)
	insertTestTxn(t, db, 1, now.Add(-10*time.Minute), 4)

	job := createTestJob(t, repo)
	if _, _, err := repo.SnapshotPlan(ctx, job.ID, now.Add(-1*time.Hour), now); err != nil {
		t.Fatalf("snapshot: %v", err)
	}

	stx, tx := beginTestTx(ctx, t, db)
	if err := repo.MarkTransactionApplied(ctx, stx, job.ID, 1); err != nil {
		tx.Rollback()
		t.Fatalf("mark applied: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	row, err := repo.GetJobTransaction(ctx, job.ID, 1)
	if err != nil {
		t.Fatalf("get job transaction: %v", err)
	}
	if row.Status != domain.RevertTransactionStatusApplied {
		t.Errorf("expected applied, got %s", row.Status)
	}
	if row.AppliedAt == nil {
		t.Error("expected applied_at to be set")
	}

	updated, _ := repo.GetJob(ctx, job.ID)
	if updated.CompletedCount != 4 {
		t.Errorf("expected completed_count=4, got %d", updated.CompletedCount)
	}
}

func TestMarkTransactionFailed(t *testing.T) {
	ctx := context.Background()
	pgContainer, db := startPGWithRevertTables(ctx, t)
	defer func() { pgContainer.Terminate(ctx) }()
	defer db.Close()

	repo := postgres.NewRevertRepository(db)

	now := time.Now().UTC().Truncate(time.Microsecond)
	insertTestTxn(t, db, 1, now.Add(-10*time.Minute), 4)

	job := createTestJob(t, repo)
	if _, _, err := repo.SnapshotPlan(ctx, job.ID, now.Add(-1*time.Hour), now); err != nil {
		t.Fatalf("snapshot: %v", err)
	}

	stx, tx := beginTestTx(ctx, t, db)
	if err := repo.MarkTransactionFailed(ctx, stx, job.ID, 1, "exec error"); err != nil {
		tx.Rollback()
		t.Fatalf("mark failed: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	row, err := repo.GetJobTransaction(ctx, job.ID, 1)
	if err != nil {
		t.Fatalf("get job transaction: %v", err)
	}
	if row.Status != domain.RevertTransactionStatusFailed {
		t.Errorf("expected failed, got %s", row.Status)
	}
	if row.ErrorMessage != "exec error" {
		t.Errorf("expected error message, got %q", row.ErrorMessage)
	}

	updated, _ := repo.GetJob(ctx, job.ID)
	if updated.FailedCount != 4 {
		t.Errorf("expected failed_count=4, got %d", updated.FailedCount)
	}
}

func TestListPendingJobIDs(t *testing.T) {
	ctx := context.Background()
	pgContainer, db := startPGWithRevertTables(ctx, t)
	defer func() { pgContainer.Terminate(ctx) }()
	defer db.Close()

	repo := postgres.NewRevertRepository(db)

	job1 := createTestJob(t, repo)
	time.Sleep(10 * time.Millisecond) // distinct created_at for ordering
	job2 := createTestJob(t, repo)
	job2.Status = domain.RevertJobStatusInProgress
	if err := repo.UpdateJob(ctx, job2); err != nil {
		t.Fatalf("update job2: %v", err)
	}
	time.Sleep(10 * time.Millisecond)
	job3 := createTestJob(t, repo)

	ids, err := repo.ListPendingJobIDs(ctx)
	if err != nil {
		t.Fatalf("list pending: %v", err)
	}
	if len(ids) != 2 {
		t.Fatalf("expected 2 pending jobs, got %d", len(ids))
	}
	if ids[0] != job1.ID || ids[1] != job3.ID {
		t.Errorf("expected [%s %s] oldest-first, got %v", job1.ID, job3.ID, ids)
	}
}

// insertTestChangeWithRows inserts a change with the row payloads a preview
// needs (old_row/new_row as JSONB).
func insertTestChangeWithRows(t *testing.T, db *sqlx.DB, id, txnID uint64, seq int32, tableName, op string, oldRow, newRow map[string]any, reverseSQL string) {
	t.Helper()
	var oldJSON, newJSON []byte
	var err error
	if oldRow != nil {
		if oldJSON, err = json.Marshal(oldRow); err != nil {
			t.Fatalf("marshal old row: %v", err)
		}
	}
	if newRow != nil {
		if newJSON, err = json.Marshal(newRow); err != nil {
			t.Fatalf("marshal new row: %v", err)
		}
	}
	_, err = db.Exec(
		`INSERT INTO wal_change (id, transaction_id, change_seq_in_txn, schema_name, table_name, op, changed_columns, old_row, new_row, forward_dml_sql, reverse_dml_sql)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
		id, txnID, seq, "public", tableName, op, pq.StringArray{"status"}, oldJSON, newJSON, reverseSQL, reverseSQL,
	)
	if err != nil {
		t.Fatalf("insert change %d: %v", id, err)
	}
}

func TestListRevertableChanges_OrderAndPayloads(t *testing.T) {
	ctx := context.Background()
	pgContainer, db := startPGWithRevertTables(ctx, t)
	defer func() { pgContainer.Terminate(ctx) }()
	defer db.Close()

	repo := postgres.NewRevertRepository(db)

	now := time.Now().UTC().Truncate(time.Microsecond)
	from := now.Add(-1 * time.Hour)
	to := now

	// txn 1 older (lower LSN), txn 2 newer. Revert order = newest first,
	// and within a txn change_seq DESC.
	insertTestTxn(t, db, 1, now.Add(-30*time.Minute), 1)
	insertTestChangeWithRows(t, db, 10, 1, 1, "orders", "U",
		map[string]any{"id": "17", "status": "processing"},
		map[string]any{"id": "17", "status": "paid"},
		`UPDATE public.orders SET status = 'processing' WHERE id = '17'`)
	insertTestTxn(t, db, 2, now.Add(-20*time.Minute), 2)
	insertTestChangeWithRows(t, db, 20, 2, 1, "orders", "I",
		nil, map[string]any{"id": "42", "status": "pending"},
		`DELETE FROM public.orders WHERE id = '42'`)
	insertTestChangeWithRows(t, db, 21, 2, 2, "customers", "D",
		map[string]any{"id": "7", "name": "Ada"}, nil,
		`INSERT INTO public.customers (id, name) VALUES ('7', 'Ada') ON CONFLICT DO NOTHING`)

	changes, err := repo.ListRevertableChanges(ctx, from, to)
	if err != nil {
		t.Fatalf("ListRevertableChanges: %v", err)
	}
	if len(changes) != 3 {
		t.Fatalf("changes = %d, want 3", len(changes))
	}

	// commit_lsn DESC, then change_seq_in_txn DESC.
	wantOrder := []uint64{21, 20, 10}
	for i, id := range wantOrder {
		if changes[i].ID != id {
			t.Fatalf("order[%d] = change %d, want %d", i, changes[i].ID, id)
		}
	}

	// Row payloads round-trip.
	if changes[2].OldRow["status"] != "processing" || changes[2].NewRow["status"] != "paid" {
		t.Errorf("change 10 rows: old=%v new=%v", changes[2].OldRow, changes[2].NewRow)
	}
	if changes[0].NewRow != nil || changes[0].OldRow["name"] != "Ada" {
		t.Errorf("change 21 rows: old=%v new=%v", changes[0].OldRow, changes[0].NewRow)
	}
	if len(changes[2].ChangedColumns) != 1 || changes[2].ChangedColumns[0] != "status" {
		t.Errorf("changed_columns = %v", changes[2].ChangedColumns)
	}

	// After txn 2 is applied by a job, its changes drop out of the window.
	job := createTestJob(t, repo)
	if _, _, err := repo.SnapshotPlan(ctx, job.ID, from, to); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if _, err := db.Exec(
		`UPDATE revert_job_transaction SET status = 'applied', applied_at = now() WHERE job_id = $1 AND seq = 1`,
		job.ID); err != nil {
		t.Fatalf("mark applied: %v", err)
	}

	changes, err = repo.ListRevertableChanges(ctx, from, to)
	if err != nil {
		t.Fatalf("ListRevertableChanges after apply: %v", err)
	}
	if len(changes) != 1 || changes[0].ID != 10 {
		t.Errorf("after apply: changes = %+v, want only change 10", changes)
	}
}

func TestRevertRepository_ImplementsPort(t *testing.T) {
	ctx := context.Background()
	pgContainer, db := startPGWithRevertTables(ctx, t)
	defer func() { pgContainer.Terminate(ctx) }()
	defer db.Close()

	repo := postgres.NewRevertRepository(db)
	var _ outbound.RevertRepository = repo
	_ = ctx
}
