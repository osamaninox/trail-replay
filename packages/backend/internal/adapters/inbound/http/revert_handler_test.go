package httphandler_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	httphandler "trail-replay/internal/adapters/inbound/http"
	"trail-replay/internal/adapters/outbound/storage/postgres"
	"trail-replay/internal/core/trail/domain"
	"trail-replay/internal/core/trail/services"
)

func startPGWithRevert(ctx context.Context, t *testing.T) (*tcpostgres.PostgresContainer, *sqlx.DB) {
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

	if err := runRevertHandlerMigrations(db); err != nil {
		t.Fatalf("failed to run migrations: %v", err)
	}

	return pgContainer, db
}

func runRevertHandlerMigrations(db *sqlx.DB) error {
	statements := []string{
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
		`CREATE TABLE wal_transaction (
			id bigserial primary key, source_slot text, source_db text, xid bigint,
			commit_lsn pg_lsn, commit_ts timestamptz, change_count int, ingested_at timestamptz
		)`,
		`CREATE TABLE wal_change (
			id bigserial primary key, transaction_id bigint references wal_transaction(id),
			change_seq_in_txn int, schema_name text, table_name text, op char(1),
			forward_dml_sql text not null default '', reverse_dml_sql text not null default '',
			undo_status text not null default 'not_applied',
			changed_columns text[] not null default '{}', created_at timestamptz not null default now()
		)`,
		`CREATE TABLE revert_job_change (
			job_id uuid references revert_job(id) on delete cascade,
			change_id bigint references wal_change(id),
			status text not null default 'pending',
			error_message text,
			applied_at timestamptz,
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

func setupRevertHandler(db *sqlx.DB) *httphandler.RevertHandler {
	repo := postgres.NewRevertRepository(db)
	svc := services.NewRevertService(repo, nil)
	return httphandler.NewRevertHandler(svc)
}

func TestCreateRevertJob_Success(t *testing.T) {
	ctx := context.Background()
	pgContainer, db := startPGWithRevert(ctx, t)
	defer func() { pgContainer.Terminate(ctx) }()
	defer db.Close()

	h := setupRevertHandler(db)

	from := time.Now().Add(-1 * time.Hour).UTC()
	to := time.Now().UTC()
	body := fmt.Sprintf(`{"from":"%s","to":"%s"}`, from.Format(time.RFC3339Nano), to.Format(time.RFC3339Nano))

	req := httptest.NewRequest(http.MethodPost, "/reverts/jobs", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	mux := http.NewServeMux()
	h.RegisterRoutes(mux)
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp domain.RevertJobResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Status != "pending" {
		t.Errorf("expected pending, got %s", resp.Status)
	}
	if resp.ID == uuid.Nil {
		t.Error("expected non-nil id")
	}
}

func TestCreateRevertJob_InvalidBody(t *testing.T) {
	ctx := context.Background()
	pgContainer, db := startPGWithRevert(ctx, t)
	defer func() { pgContainer.Terminate(ctx) }()
	defer db.Close()

	h := setupRevertHandler(db)

	req := httptest.NewRequest(http.MethodPost, "/reverts/jobs", strings.NewReader(`{"bad":1}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	mux := http.NewServeMux()
	h.RegisterRoutes(mux)
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", rec.Code)
	}
}

func TestCreateRevertJob_InvalidTimeRange(t *testing.T) {
	ctx := context.Background()
	pgContainer, db := startPGWithRevert(ctx, t)
	defer func() { pgContainer.Terminate(ctx) }()
	defer db.Close()

	h := setupRevertHandler(db)

	now := time.Now()
	body := fmt.Sprintf(`{"from":"%s","to":"%s"}`, now.Format(time.RFC3339Nano), now.Add(-1*time.Hour).Format(time.RFC3339Nano))

	req := httptest.NewRequest(http.MethodPost, "/reverts/jobs", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	mux := http.NewServeMux()
	h.RegisterRoutes(mux)
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", rec.Code)
	}
}

func TestGetRevertJob_Success(t *testing.T) {
	ctx := context.Background()
	pgContainer, db := startPGWithRevert(ctx, t)
	defer func() { pgContainer.Terminate(ctx) }()
	defer db.Close()

	h := setupRevertHandler(db)

	from := time.Now().Add(-1 * time.Hour).UTC()
	to := time.Now().UTC()
	body := fmt.Sprintf(`{"from":"%s","to":"%s"}`, from.Format(time.RFC3339Nano), to.Format(time.RFC3339Nano))

	createReq := httptest.NewRequest(http.MethodPost, "/reverts/jobs", strings.NewReader(body))
	createReq.Header.Set("Content-Type", "application/json")
	createRec := httptest.NewRecorder()

	mux := http.NewServeMux()
	h.RegisterRoutes(mux)
	mux.ServeHTTP(createRec, createReq)

	var created domain.RevertJobResponse
	json.NewDecoder(createRec.Body).Decode(&created)

	getReq := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/reverts/jobs/%s", created.ID), nil)
	getRec := httptest.NewRecorder()
	mux.ServeHTTP(getRec, getReq)

	if getRec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", getRec.Code)
	}

	var got domain.RevertJobResponse
	json.NewDecoder(getRec.Body).Decode(&got)
	if got.ID != created.ID {
		t.Errorf("id mismatch")
	}
}

func TestGetRevertJob_NotFound(t *testing.T) {
	ctx := context.Background()
	pgContainer, db := startPGWithRevert(ctx, t)
	defer func() { pgContainer.Terminate(ctx) }()
	defer db.Close()

	h := setupRevertHandler(db)

	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/reverts/jobs/%s", uuid.New()), nil)
	rec := httptest.NewRecorder()

	mux := http.NewServeMux()
	h.RegisterRoutes(mux)
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Errorf("expected 404, got %d", rec.Code)
	}
}

func TestListRevertJobs_Success(t *testing.T) {
	ctx := context.Background()
	pgContainer, db := startPGWithRevert(ctx, t)
	defer func() { pgContainer.Terminate(ctx) }()
	defer db.Close()

	h := setupRevertHandler(db)

	from := time.Now().Add(-1 * time.Hour).UTC()
	to := time.Now().UTC()
	body := fmt.Sprintf(`{"from":"%s","to":"%s"}`, from.Format(time.RFC3339Nano), to.Format(time.RFC3339Nano))

	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	createReq := httptest.NewRequest(http.MethodPost, "/reverts/jobs", strings.NewReader(body))
	createReq.Header.Set("Content-Type", "application/json")
	mux.ServeHTTP(httptest.NewRecorder(), createReq)

	listReq := httptest.NewRequest(http.MethodGet, "/reverts/jobs", nil)
	listRec := httptest.NewRecorder()
	mux.ServeHTTP(listRec, listReq)

	if listRec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", listRec.Code)
	}

	var resp domain.PaginatedResponse[domain.RevertJobResponse]
	json.NewDecoder(listRec.Body).Decode(&resp)
	if len(resp.Data) != 1 {
		t.Errorf("expected 1 job, got %d", len(resp.Data))
	}
	if resp.TotalCount != 1 {
		t.Errorf("expected TotalCount=1, got %d", resp.TotalCount)
	}
	if resp.Page != 1 {
		t.Errorf("expected Page=1, got %d", resp.Page)
	}
}

func TestCancelRevertJob_Success(t *testing.T) {
	ctx := context.Background()
	pgContainer, db := startPGWithRevert(ctx, t)
	defer func() { pgContainer.Terminate(ctx) }()
	defer db.Close()

	h := setupRevertHandler(db)

	from := time.Now().Add(-1 * time.Hour).UTC()
	to := time.Now().UTC()
	body := fmt.Sprintf(`{"from":"%s","to":"%s"}`, from.Format(time.RFC3339Nano), to.Format(time.RFC3339Nano))

	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	createReq := httptest.NewRequest(http.MethodPost, "/reverts/jobs", strings.NewReader(body))
	createReq.Header.Set("Content-Type", "application/json")
	createRec := httptest.NewRecorder()
	mux.ServeHTTP(createRec, createReq)

	var created domain.RevertJobResponse
	json.NewDecoder(createRec.Body).Decode(&created)

	cancelReq := httptest.NewRequest(http.MethodDelete, fmt.Sprintf("/reverts/jobs/%s", created.ID), nil)
	cancelRec := httptest.NewRecorder()
	mux.ServeHTTP(cancelRec, cancelReq)

	if cancelRec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", cancelRec.Code)
	}

	var cancelled domain.RevertJobResponse
	json.NewDecoder(cancelRec.Body).Decode(&cancelled)
	if cancelled.Status != "cancelled" {
		t.Errorf("expected cancelled, got %s", cancelled.Status)
	}
}

func TestCancelRevertJob_Completed(t *testing.T) {
	ctx := context.Background()
	pgContainer, db := startPGWithRevert(ctx, t)
	defer func() { pgContainer.Terminate(ctx) }()
	defer db.Close()

	h := setupRevertHandler(db)

	from := time.Now().Add(-1 * time.Hour).UTC()
	to := time.Now().UTC()
	body := fmt.Sprintf(`{"from":"%s","to":"%s"}`, from.Format(time.RFC3339Nano), to.Format(time.RFC3339Nano))

	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	createReq := httptest.NewRequest(http.MethodPost, "/reverts/jobs", strings.NewReader(body))
	createReq.Header.Set("Content-Type", "application/json")
	createRec := httptest.NewRecorder()
	mux.ServeHTTP(createRec, createReq)

	var created domain.RevertJobResponse
	json.NewDecoder(createRec.Body).Decode(&created)

	db.Exec("UPDATE revert_job SET status = 'completed' WHERE id = $1", created.ID)

	cancelReq := httptest.NewRequest(http.MethodDelete, fmt.Sprintf("/reverts/jobs/%s", created.ID), nil)
	cancelRec := httptest.NewRecorder()
	mux.ServeHTTP(cancelRec, cancelReq)

	if cancelRec.Code != http.StatusConflict {
		t.Errorf("expected 409, got %d", cancelRec.Code)
	}
}
