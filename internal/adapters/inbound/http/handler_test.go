package httphandler_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	httphandler "github.com/osamakhalid/trail-replay/internal/adapters/inbound/http"
	"github.com/osamakhalid/trail-replay/internal/adapters/outbound/storage"
	"github.com/osamakhalid/trail-replay/internal/adapters/outbound/storage/postgres"
	"github.com/osamakhalid/trail-replay/internal/core/trail/domain"
	"github.com/osamakhalid/trail-replay/internal/core/trail/services"
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

func startPG(ctx context.Context, t *testing.T) (*tcpostgres.PostgresContainer, *sqlx.DB) {
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

	if err := runMigrations(db); err != nil {
		t.Fatalf("failed to run migrations: %v", err)
	}

	return pgContainer, db
}

func runMigrations(db *sqlx.DB) error {
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
	}
	for _, s := range statements {
		if _, err := db.Exec(s); err != nil {
			return fmt.Errorf("migration error: %w\nSQL: %s", err, s)
		}
	}
	return nil
}

func insertTxn(db *sqlx.DB, txn domain.WalTransactionResult) error {
	lsn := fmt.Sprintf("0/%X", txn.ID)
	_, err := db.Exec(
		`INSERT INTO wal_transaction (id, source_slot, source_db, xid, commit_lsn, commit_ts, change_count, ingested_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		txn.ID, txn.SourceSlot, txn.SourceDb, txn.Xid, lsn, txn.CommitTS, txn.ChangeCount, txn.IngestedAt,
	)
	if err != nil {
		return err
	}
	for _, c := range txn.Changes {
		_, err := db.Exec(
			`INSERT INTO wal_change (id, transaction_id, change_seq_in_txn, schema_name, table_name, op, changed_columns, forward_dml_sql, reverse_dml_sql, undo_status, created_at)
			 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
			c.ID, c.TransactionID, c.ChangeSeqInTxn, c.SchemaName, c.TableName, c.Op,
			"{"+joinCols(c.ChangedColumns)+"}",
			c.ForwardDMLSQL, c.ReverseDMLSQL, c.UndoStatus, c.CreatedAt,
		)
		if err != nil {
			return err
		}
	}
	return nil
}

func joinCols(cols []string) string {
	if len(cols) == 0 {
		return ""
	}
	s := cols[0]
	for i := 1; i < len(cols); i++ {
		s += "," + cols[i]
	}
	return s
}

func setupIntegrationHandler(db *sqlx.DB) *httphandler.Handler {
	trailRepo := storage.NewInMemoryRepository()
	trailSvc := services.NewTrailService(trailRepo)
	walRepo := postgres.NewWalQueryRepository(db)
	walSvc := services.NewWalQueryService(walRepo)
	return httphandler.NewHandler(trailSvc, walSvc)
}

func TestListWalTransactions_EmptyDB(t *testing.T) {
	ctx := context.Background()
	pgContainer, db := startPG(ctx, t)
	defer func() { pgContainer.Terminate(ctx) }()
	defer db.Close()

	h := setupIntegrationHandler(db)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	req := httptest.NewRequest(http.MethodGet, "/wal/transactions", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp domain.PaginatedResponse[domain.WalTransactionResult]
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.TotalCount != 0 {
		t.Errorf("TotalCount=0, got %d", resp.TotalCount)
	}
	if resp.TotalPages != 0 {
		t.Errorf("TotalPages=0, got %d", resp.TotalPages)
	}
	if resp.Page != 1 {
		t.Errorf("Page=1, got %d", resp.Page)
	}
	if len(resp.Data) != 0 {
		t.Errorf("Data empty, got %d", len(resp.Data))
	}
}

func TestListWalTransactions_WithData(t *testing.T) {
	ctx := context.Background()
	pgContainer, db := startPG(ctx, t)
	defer func() { pgContainer.Terminate(ctx) }()
	defer db.Close()

	now := time.Now().UTC().Truncate(time.Microsecond)
	txn := domain.WalTransactionResult{
		ID:          1,
		SourceSlot:  "trail_replay_poc_slot",
		SourceDb:    "traildb",
		Xid:         12345,
		CommitTS:    now,
		ChangeCount: 2,
		IngestedAt:  now,
		Changes: []domain.WalChangeResult{
			{
				ID:             1,
				TransactionID:  1,
				ChangeSeqInTxn: 1,
				SchemaName:     "public",
				TableName:      "users",
				Op:             "U",
				ChangedColumns: []string{"email"},
				ForwardDMLSQL:  "UPDATE users SET email='new' WHERE id=1",
				ReverseDMLSQL:  "UPDATE users SET email='old' WHERE id=1",
				UndoStatus:     "pending",
				CreatedAt:      now,
			},
			{
				ID:             2,
				TransactionID:  1,
				ChangeSeqInTxn: 2,
				SchemaName:     "public",
				TableName:      "orders",
				Op:             "I",
				ChangedColumns: []string{"id", "total"},
				ForwardDMLSQL:  "INSERT INTO orders (id, total) VALUES (1, 99)",
				ReverseDMLSQL:  "DELETE FROM orders WHERE id=1",
				UndoStatus:     "pending",
				CreatedAt:      now,
			},
		},
	}
	if err := insertTxn(db, txn); err != nil {
		t.Fatalf("insert: %v", err)
	}

	h := setupIntegrationHandler(db)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	req := httptest.NewRequest(http.MethodGet, "/wal/transactions", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var resp domain.PaginatedResponse[domain.WalTransactionResult]
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.TotalCount != 1 {
		t.Errorf("TotalCount=1, got %d", resp.TotalCount)
	}
	if resp.TotalPages != 1 {
		t.Errorf("TotalPages=1, got %d", resp.TotalPages)
	}
	if resp.Page != 1 {
		t.Errorf("Page=1, got %d", resp.Page)
	}
	if resp.PageSize != 20 {
		t.Errorf("PageSize=20, got %d", resp.PageSize)
	}
	if len(resp.Data) != 1 {
		t.Fatalf("len(Data)=1, got %d", len(resp.Data))
	}
	r := resp.Data[0]
	if r.ID != 1 || r.Xid != 12345 || r.SourceSlot != "trail_replay_poc_slot" || r.ChangeCount != 2 {
		t.Errorf("txn fields mismatch: id=%d xid=%d slot=%s count=%d", r.ID, r.Xid, r.SourceSlot, r.ChangeCount)
	}
	if len(r.Changes) != 2 {
		t.Fatalf("len(Changes)=2, got %d", len(r.Changes))
	}
	if r.Changes[0].ChangeSeqInTxn != 1 || r.Changes[0].TableName != "users" || r.Changes[0].Op != "U" {
		t.Errorf("change[0] mismatch")
	}
	if r.Changes[1].ChangeSeqInTxn != 2 || r.Changes[1].TableName != "orders" || r.Changes[1].Op != "I" {
		t.Errorf("change[1] mismatch")
	}
	if r.CommitLSN == nil {
		t.Error("commit_lsn should not be nil")
	}
}

func TestListWalTransactions_Pagination(t *testing.T) {
	ctx := context.Background()
	pgContainer, db := startPG(ctx, t)
	defer func() { pgContainer.Terminate(ctx) }()
	defer db.Close()

	now := time.Now().UTC().Truncate(time.Microsecond)
	for i := 1; i <= 25; i++ {
		txn := domain.WalTransactionResult{
			ID:          uint64(i),
			SourceSlot:  "slot",
			SourceDb:    "db",
			Xid:         int64(i * 100),
			CommitTS:    now.Add(time.Duration(i) * time.Second),
			ChangeCount: 1,
			IngestedAt:  now.Add(time.Duration(i) * time.Second),
			Changes: []domain.WalChangeResult{
				{
					ID:              uint64(i),
					TransactionID:  uint64(i),
					ChangeSeqInTxn: 1,
					SchemaName:     "public",
					TableName:      "test",
					Op:             "U",
					ForwardDMLSQL:  fmt.Sprintf("UPDATE test SET x=%d", i),
					ReverseDMLSQL:  fmt.Sprintf("UPDATE test SET x=old_%d", i),
					UndoStatus:     "pending",
					CreatedAt:      now,
				},
			},
		}
		if err := insertTxn(db, txn); err != nil {
			t.Fatalf("insert txn %d: %v", i, err)
		}
	}

	h := setupIntegrationHandler(db)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	req := httptest.NewRequest(http.MethodGet, "/wal/transactions?page=1&page_size=10", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	var p1 domain.PaginatedResponse[domain.WalTransactionResult]
	json.NewDecoder(rec.Body).Decode(&p1)

	if p1.Page != 1 || p1.PageSize != 10 || p1.TotalCount != 25 || p1.TotalPages != 3 {
		t.Errorf("page1: page=%d size=%d total=%d pages=%d", p1.Page, p1.PageSize, p1.TotalCount, p1.TotalPages)
	}
	if len(p1.Data) != 10 {
		t.Fatalf("page1: expected 10 items, got %d", len(p1.Data))
	}

	req3 := httptest.NewRequest(http.MethodGet, "/wal/transactions?page=3&page_size=10", nil)
	rec3 := httptest.NewRecorder()
	mux.ServeHTTP(rec3, req3)

	var p3 domain.PaginatedResponse[domain.WalTransactionResult]
	json.NewDecoder(rec3.Body).Decode(&p3)

	if p3.Page != 3 {
		t.Errorf("page3: page=%d", p3.Page)
	}
	if len(p3.Data) != 5 {
		t.Fatalf("page3: expected 5 items, got %d", len(p3.Data))
	}
}

func TestListWalTransactions_SortOrder(t *testing.T) {
	ctx := context.Background()
	pgContainer, db := startPG(ctx, t)
	defer func() { pgContainer.Terminate(ctx) }()
	defer db.Close()

	now := time.Now().UTC().Truncate(time.Microsecond)
	ids := []uint64{1, 2, 3}
	offsets := []time.Duration{-2 * time.Hour, 0, -1 * time.Hour}

	for i, id := range ids {
		txn := domain.WalTransactionResult{
			ID:          id,
			SourceSlot:  "s",
			SourceDb:    "d",
			Xid:         int64(id * 100),
			CommitTS:    now.Add(offsets[i]),
			ChangeCount: 1,
			IngestedAt:  now.Add(offsets[i]),
			Changes:     []domain.WalChangeResult{},
		}
		if err := insertTxn(db, txn); err != nil {
			t.Fatalf("insert txn %d: %v", id, err)
		}
	}

	h := setupIntegrationHandler(db)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	req := httptest.NewRequest(http.MethodGet, "/wal/transactions?page_size=10", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	var resp domain.PaginatedResponse[domain.WalTransactionResult]
	json.NewDecoder(rec.Body).Decode(&resp)

	if len(resp.Data) != 3 {
		t.Fatalf("expected 3 txns, got %d", len(resp.Data))
	}
	if resp.Data[0].ID != 2 {
		t.Errorf("first: id=2 (most recent), got %d", resp.Data[0].ID)
	}
	if resp.Data[1].ID != 3 {
		t.Errorf("second: id=3, got %d", resp.Data[1].ID)
	}
	if resp.Data[2].ID != 1 {
		t.Errorf("third: id=1 (oldest), got %d", resp.Data[2].ID)
	}
}

func TestListWalTransactions_BeyondLastPage(t *testing.T) {
	ctx := context.Background()
	pgContainer, db := startPG(ctx, t)
	defer func() { pgContainer.Terminate(ctx) }()
	defer db.Close()

	now := time.Now().UTC().Truncate(time.Microsecond)
	txn := domain.WalTransactionResult{
		ID:          1,
		SourceSlot:  "s",
		SourceDb:    "d",
		Xid:         100,
		CommitTS:    now,
		ChangeCount: 1,
		IngestedAt:  now,
		Changes:     []domain.WalChangeResult{},
	}
	insertTxn(db, txn)

	h := setupIntegrationHandler(db)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	req := httptest.NewRequest(http.MethodGet, "/wal/transactions?page=10&page_size=20", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	var resp domain.PaginatedResponse[domain.WalTransactionResult]
	json.NewDecoder(rec.Body).Decode(&resp)

	if len(resp.Data) != 0 {
		t.Errorf("expected 0 items beyond last page, got %d", len(resp.Data))
	}
	if resp.TotalCount != 1 {
		t.Errorf("TotalCount=1, got %d", resp.TotalCount)
	}
}
