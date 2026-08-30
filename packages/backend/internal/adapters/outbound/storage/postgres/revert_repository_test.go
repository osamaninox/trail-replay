package postgres_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
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

func runRevertMigrations(db *sqlx.DB) error {
	statements := []string{
		`CREATE TABLE wal_transaction (
			id                bigserial primary key,
			source_slot       text not null,
			source_db         text not null,
			xid               bigint not null,
			begin_lsn         pg_lsn,
			commit_lsn        pg_lsn not null,
			end_lsn           pg_lsn,
			commit_ts         timestamptz not null,
			origin_name       text,
			tenant_id         text,
			change_count      integer not null default 0,
			forward_sql_text  text,
			reverse_sql_text  text,
			forward_sql_hash  bytea,
			reverse_sql_hash  bytea,
			safety_flags      text[] not null default '{}',
			ingest_batch_id   uuid,
			ingested_at       timestamptz not null default now(),
			unique (source_slot, commit_lsn),
			unique (source_slot, xid, commit_lsn)
		)`,
		`CREATE TABLE wal_change (
			id                    bigserial primary key,
			transaction_id        bigint not null references wal_transaction(id) on delete cascade,
			change_seq_in_txn     integer not null,
			change_lsn            pg_lsn,
			tenant_id             text,
			schema_name           text not null,
			table_name            text not null,
			table_oid             oid,
			op                    char(1) not null check (op in ('I','U','D')),
			replica_identity_mode text,
			row_pk                jsonb,
			old_row               jsonb,
			new_row               jsonb,
			changed_columns       text[] not null default '{}',
			forward_dml_sql       text not null,
			reverse_dml_sql       text not null,
			forward_sql_hash      bytea,
			reverse_sql_hash      bytea,
			reverse_where_sql     text,
			affected_row_count    integer not null default 1,
			undo_status           text not null default 'not_applied',
			undo_applied_at       timestamptz,
			undo_applied_by       text,
			safety_flags          text[] not null default '{}',
			created_at            timestamptz not null default now(),
			unique (transaction_id, change_seq_in_txn)
		)`,
		`CREATE TABLE revert_job (
			id              uuid primary key,
			status          text not null default 'pending',
			input_from      timestamptz not null,
			input_to        timestamptz not null,
			total_changes   int not null default 0,
			completed_count int not null default 0,
			failed_count    int not null default 0,
			last_error      text,
			created_at      timestamptz not null default now(),
			updated_at      timestamptz not null default now(),
			completed_at    timestamptz
		)`,
		`CREATE TABLE revert_job_change (
			job_id        uuid references revert_job(id) on delete cascade,
			change_id     bigint references wal_change(id),
			status        text not null default 'pending',
			error_message text,
			applied_at    timestamptz,
			primary key (job_id, change_id)
		)`,
	}
	for _, s := range statements {
		if _, err := db.Exec(s); err != nil {
			return fmt.Errorf("migration error: %w\nSQL: %s", err, s)
		}
	}
	return nil
}

func insertTestTxn(db *sqlx.DB, id uint64, commitTS time.Time) error {
	lsn := fmt.Sprintf("0/%X", id)
	_, err := db.Exec(
		`INSERT INTO wal_transaction (id, source_slot, source_db, xid, commit_lsn, commit_ts, change_count, ingested_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		id, "test_slot", "testdb", int64(id*100), lsn, commitTS, 1, commitTS,
	)
	return err
}

func insertTestChange(db *sqlx.DB, id uint64, txnID uint64, seq int32, tableName, op, reverseSQL, undoStatus string) error {
	_, err := db.Exec(
		`INSERT INTO wal_change (id, transaction_id, change_seq_in_txn, schema_name, table_name, op, forward_dml_sql, reverse_dml_sql, undo_status)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		id, txnID, seq, "public", tableName, op, reverseSQL, reverseSQL, undoStatus,
	)
	return err
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

func TestGetNextPendingJob(t *testing.T) {
	ctx := context.Background()
	pgContainer, db := startPGWithRevertTables(ctx, t)
	defer func() { pgContainer.Terminate(ctx) }()
	defer db.Close()

	repo := postgres.NewRevertRepository(db)

	from := time.Now().Add(-1 * time.Hour).UTC().Truncate(time.Microsecond)
	to := time.Now().UTC().Truncate(time.Microsecond)

	job1 := domain.NewRevertJob(from, to)
	repo.CreateJob(ctx, job1)

	time.Sleep(time.Millisecond)
	job2 := domain.NewRevertJob(from, to)
	repo.CreateJob(ctx, job2)

	next, err := repo.GetNextPendingJob(ctx)
	if err != nil {
		t.Fatalf("get next pending: %v", err)
	}
	if next == nil {
		t.Fatal("expected a pending job")
	}
	if next.ID != job1.ID {
		t.Errorf("expected oldest pending (%s), got %s", job1.ID, next.ID)
	}
}

func TestGetNextPendingJob_None(t *testing.T) {
	ctx := context.Background()
	pgContainer, db := startPGWithRevertTables(ctx, t)
	defer func() { pgContainer.Terminate(ctx) }()
	defer db.Close()

	repo := postgres.NewRevertRepository(db)

	next, err := repo.GetNextPendingJob(ctx)
	if err != nil {
		t.Fatalf("get next pending: %v", err)
	}
	if next != nil {
		t.Error("expected nil when no pending jobs")
	}
}

func TestGetActiveJob(t *testing.T) {
	ctx := context.Background()
	pgContainer, db := startPGWithRevertTables(ctx, t)
	defer func() { pgContainer.Terminate(ctx) }()
	defer db.Close()

	repo := postgres.NewRevertRepository(db)

	from := time.Now().Add(-1 * time.Hour).UTC().Truncate(time.Microsecond)
	to := time.Now().UTC().Truncate(time.Microsecond)
	job := domain.NewRevertJob(from, to)
	job.Status = domain.RevertJobStatusInProgress
	repo.CreateJob(ctx, job)

	active, err := repo.GetActiveJob(ctx)
	if err != nil {
		t.Fatalf("get active: %v", err)
	}
	if active == nil {
		t.Fatal("expected active job")
	}
	if active.ID != job.ID {
		t.Errorf("id mismatch")
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

func TestCountUnappliedChanges(t *testing.T) {
	ctx := context.Background()
	pgContainer, db := startPGWithRevertTables(ctx, t)
	defer func() { pgContainer.Terminate(ctx) }()
	defer db.Close()

	repo := postgres.NewRevertRepository(db)

	now := time.Now().UTC().Truncate(time.Microsecond)
	insertTestTxn(db, 1, now.Add(-30*time.Minute))
	insertTestTxn(db, 2, now.Add(-20*time.Minute))
	insertTestTxn(db, 3, now.Add(-5*time.Minute))

	insertTestChange(db, 1, 1, 1, "users", "U", "UPDATE users SET email='old' WHERE id=1", "not_applied")
	insertTestChange(db, 2, 2, 1, "orders", "I", "DELETE FROM orders WHERE id=1", "not_applied")
	insertTestChange(db, 3, 2, 2, "orders", "D", "INSERT INTO orders (id) VALUES (1)", "applied")
	insertTestChange(db, 4, 3, 1, "users", "U", "UPDATE users SET email='old2' WHERE id=2", "not_applied")

	from := now.Add(-1 * time.Hour)
	to := now

	count, err := repo.CountUnappliedChanges(ctx, from, to)
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 3 {
		t.Errorf("expected 3 unapplied changes (changes 1,2,4), got %d", count)
	}
}

func TestFetchUnappliedChanges(t *testing.T) {
	ctx := context.Background()
	pgContainer, db := startPGWithRevertTables(ctx, t)
	defer func() { pgContainer.Terminate(ctx) }()
	defer db.Close()

	repo := postgres.NewRevertRepository(db)

	now := time.Now().UTC().Truncate(time.Microsecond)
	insertTestTxn(db, 1, now.Add(-30*time.Minute))
	insertTestTxn(db, 2, now.Add(-10*time.Minute))

	insertTestChange(db, 1, 1, 1, "users", "U", "UPDATE users SET email='old' WHERE id=1", "not_applied")
	insertTestChange(db, 2, 2, 1, "orders", "D", "INSERT INTO orders (id) VALUES (1)", "not_applied")
	insertTestChange(db, 3, 2, 2, "orders", "I", "DELETE FROM orders WHERE id=2", "applied")

	from := now.Add(-1 * time.Hour)
	to := now

	changes, err := repo.FetchUnappliedChanges(ctx, from, to, 0, 10)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if len(changes) != 2 {
		t.Fatalf("expected 2 changes, got %d", len(changes))
	}

	if changes[0].ReverseDMLSQL != "INSERT INTO orders (id) VALUES (1)" {
		t.Errorf("expected most recent first (txn 2, seq 1), got %s", changes[0].ReverseDMLSQL)
	}
	if changes[1].ReverseDMLSQL != "UPDATE users SET email='old' WHERE id=1" {
		t.Errorf("expected older second (txn 1, seq 1), got %s", changes[1].ReverseDMLSQL)
	}
}

func TestFetchUnappliedChanges_Pagination(t *testing.T) {
	ctx := context.Background()
	pgContainer, db := startPGWithRevertTables(ctx, t)
	defer func() { pgContainer.Terminate(ctx) }()
	defer db.Close()

	repo := postgres.NewRevertRepository(db)

	now := time.Now().UTC().Truncate(time.Microsecond)
	insertTestTxn(db, 1, now.Add(-10*time.Minute))

	for i := 0; i < 5; i++ {
		insertTestChange(db, uint64(i+1), 1, int32(i+1), "users", "U", fmt.Sprintf("SQL %d", i), "not_applied")
	}

	from := now.Add(-1 * time.Hour)
	to := now

	page1, err := repo.FetchUnappliedChanges(ctx, from, to, 0, 3)
	if err != nil {
		t.Fatalf("fetch page1: %v", err)
	}
	if len(page1) != 3 {
		t.Fatalf("expected 3 changes on page 1, got %d", len(page1))
	}

	page2, err := repo.FetchUnappliedChanges(ctx, from, to, 3, 3)
	if err != nil {
		t.Fatalf("fetch page2: %v", err)
	}
	if len(page2) != 2 {
		t.Fatalf("expected 2 changes on page 2, got %d", len(page2))
	}
}

func TestMarkChangeApplied(t *testing.T) {
	ctx := context.Background()
	pgContainer, db := startPGWithRevertTables(ctx, t)
	defer func() { pgContainer.Terminate(ctx) }()
	defer db.Close()

	repo := postgres.NewRevertRepository(db)

	now := time.Now().UTC().Truncate(time.Microsecond)
	insertTestTxn(db, 1, now.Add(-10*time.Minute))
	insertTestChange(db, 1, 1, 1, "users", "U", "UPDATE users SET email='old' WHERE id=1", "not_applied")

	from := time.Now().Add(-1 * time.Hour).UTC().Truncate(time.Microsecond)
	to := time.Now().UTC().Truncate(time.Microsecond)
	job := domain.NewRevertJob(from, to)
	repo.CreateJob(ctx, job)

	change := domain.WalChangeResult{
		ID:             1,
		TransactionID:  1,
		ChangeSeqInTxn: 1,
		SchemaName:     "public",
		TableName:      "users",
		Op:             "U",
		ReverseDMLSQL:  "UPDATE users SET email='old' WHERE id=1",
	}

	if err := repo.MarkChangeApplied(ctx, job.ID, change); err != nil {
		t.Fatalf("mark applied: %v", err)
	}

	var undoStatus string
	db.Get(&undoStatus, "SELECT undo_status FROM wal_change WHERE id = $1", change.ID)
	if undoStatus != "applied" {
		t.Errorf("expected undo_status=applied, got %s", undoStatus)
	}

	var changeStatus string
	db.Get(&changeStatus, "SELECT status FROM revert_job_change WHERE job_id = $1 AND change_id = $2", job.ID, change.ID)
	if changeStatus != "applied" {
		t.Errorf("expected revert_job_change status=applied, got %s", changeStatus)
	}
}

func TestMarkChangeFailed(t *testing.T) {
	ctx := context.Background()
	pgContainer, db := startPGWithRevertTables(ctx, t)
	defer func() { pgContainer.Terminate(ctx) }()
	defer db.Close()

	repo := postgres.NewRevertRepository(db)

	now := time.Now().UTC().Truncate(time.Microsecond)
	insertTestTxn(db, 1, now.Add(-10*time.Minute))
	insertTestChange(db, 1, 1, 1, "users", "U", "UPDATE users SET email='old' WHERE id=1", "not_applied")

	from := time.Now().Add(-1 * time.Hour).UTC().Truncate(time.Microsecond)
	to := time.Now().UTC().Truncate(time.Microsecond)
	job := domain.NewRevertJob(from, to)
	repo.CreateJob(ctx, job)

	change := domain.WalChangeResult{
		ID:             1,
		TransactionID:  1,
		ChangeSeqInTxn: 1,
		ReverseDMLSQL:  "UPDATE users SET email='old' WHERE id=1",
	}

	if err := repo.MarkChangeFailed(ctx, job.ID, change, "exec error"); err != nil {
		t.Fatalf("mark failed: %v", err)
	}

	var changeStatus string
	var errMsg string
	db.Get(&errMsg, "SELECT error_message FROM revert_job_change WHERE job_id = $1 AND change_id = $2", job.ID, change.ID)
	if errMsg != "exec error" {
		t.Errorf("expected error_message='exec error', got %s", errMsg)
	}

	db.Get(&changeStatus, "SELECT status FROM revert_job_change WHERE job_id = $1 AND change_id = $2", job.ID, change.ID)
	if changeStatus != "failed" {
		t.Errorf("expected status=failed, got %s", changeStatus)
	}
}

func TestAdvisoryLock(t *testing.T) {
	ctx := context.Background()
	pgContainer, db := startPGWithRevertTables(ctx, t)
	defer func() { pgContainer.Terminate(ctx) }()
	defer db.Close()

	repo := postgres.NewRevertRepository(db)

	locked, err := repo.TryAcquireAdvisoryLock(ctx)
	if err != nil {
		t.Fatalf("try lock: %v", err)
	}
	if !locked {
		t.Fatal("expected lock to be acquired")
	}

	if err := repo.ReleaseAdvisoryLock(ctx); err != nil {
		t.Fatalf("release lock: %v", err)
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
