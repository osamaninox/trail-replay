package httphandler_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/pressly/goose/v3"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	httphandler "trail-replay/internal/adapters/inbound/http"
	"trail-replay/internal/adapters/outbound/storage/postgres"
	"trail-replay/internal/core/trail/domain"
	"trail-replay/internal/core/trail/services"
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

// runRevertHandlerMigrations applies the real goose migrations so tests
// always run against the shipped schema (go test runs with cwd = package dir).
func runRevertHandlerMigrations(db *sqlx.DB) error {
	if err := goose.SetDialect("postgres"); err != nil {
		return err
	}
	return goose.Up(db.DB, "../../../../migrations")
}

// noopSubmitter satisfies the service's RevertJobSubmitter port; handler
// tests exercise HTTP ↔ service ↔ repo and don't involve the workflow engine.
type noopSubmitter struct{}

func (noopSubmitter) Submit(context.Context, uuid.UUID) error { return nil }
func (noopSubmitter) Cancel(context.Context, uuid.UUID) error { return nil }

func setupRevertHandler(db *sqlx.DB) *httphandler.RevertHandler {
	repo := postgres.NewRevertRepository(db)
	svc := services.NewRevertService(repo, noopSubmitter{}, nil)
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

func TestListJobTransactions_Success(t *testing.T) {
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

	// Seed the plan snapshot directly (the workflow would do this at runtime).
	for seq, txnID := range []int64{20, 10} {
		_, err := db.Exec(
			`INSERT INTO revert_job_transaction (job_id, seq, transaction_id, commit_lsn, change_count, status)
			 VALUES ($1, $2, $3, $4, $5, 'pending')`,
			created.ID, seq+1, txnID, fmt.Sprintf("0/%X", txnID), 3)
		if err != nil {
			t.Fatalf("seed snapshot row: %v", err)
		}
	}

	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/reverts/jobs/%s/transactions", created.ID), nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var got domain.PaginatedResponse[domain.RevertJobTransactionResponse]
	if err := json.NewDecoder(rec.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.TotalCount != 2 || len(got.Data) != 2 {
		t.Fatalf("expected 2 transactions, got total=%d len=%d", got.TotalCount, len(got.Data))
	}
	if got.Data[0].Seq != 1 || got.Data[0].TransactionID != 20 || got.Data[0].ChangeCount != 3 {
		t.Errorf("unexpected first row: %+v", got.Data[0])
	}
	if got.Data[0].Status != "pending" {
		t.Errorf("expected status pending, got %s", got.Data[0].Status)
	}
	if got.Data[1].Seq != 2 || got.Data[1].TransactionID != 10 {
		t.Errorf("unexpected second row: %+v", got.Data[1])
	}
}

func TestListJobTransactions_NotFound(t *testing.T) {
	ctx := context.Background()
	pgContainer, db := startPGWithRevert(ctx, t)
	defer func() { pgContainer.Terminate(ctx) }()
	defer db.Close()

	h := setupRevertHandler(db)

	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/reverts/jobs/%s/transactions", uuid.New()), nil)
	rec := httptest.NewRecorder()

	mux := http.NewServeMux()
	h.RegisterRoutes(mux)
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Errorf("expected 404, got %d", rec.Code)
	}
}
