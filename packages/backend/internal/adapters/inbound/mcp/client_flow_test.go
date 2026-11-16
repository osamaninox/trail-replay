package mcp

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"trail-replay/internal/core/trail/domain"
)

func TestLogin_Success(t *testing.T) {
	api := newFakeAPI(t)
	client := newTestClient(t, api)

	if err := client.Login(context.Background()); err != nil {
		t.Fatalf("Login: %v", err)
	}
	if api.loginCalls.Load() != 1 {
		t.Fatalf("login calls = %d, want 1", api.loginCalls.Load())
	}
}

func TestLogin_BadCredentials(t *testing.T) {
	api := newFakeAPI(t)
	srv := httptest.NewServer(api.handler())
	t.Cleanup(srv.Close)

	client := NewClient(srv.URL, api.validEmail, "wrong-password")

	err := client.Login(context.Background())
	if err == nil || !strings.Contains(err.Error(), "invalid credentials") {
		t.Fatalf("Login error = %v, want it to contain %q", err, "invalid credentials")
	}
}

func TestCreateRevertJob_SendsBearerToken(t *testing.T) {
	api := newFakeAPI(t)
	client := newTestClient(t, api)
	ctx := context.Background()

	if err := client.Login(ctx); err != nil {
		t.Fatalf("Login: %v", err)
	}

	from := time.Date(2026, 11, 1, 14, 0, 0, 0, time.UTC)
	to := from.Add(10 * time.Minute)
	job, err := client.CreateRevertJob(ctx, from, to)
	if err != nil {
		t.Fatalf("CreateRevertJob: %v", err)
	}
	if job.ID.String() != "11111111-2222-3333-4444-555555555555" {
		t.Errorf("job ID = %s, want fixed fake ID", job.ID)
	}
	if !job.InputFrom.Equal(from) || !job.InputTo.Equal(to) {
		t.Errorf("window = %s..%s, want %s..%s", job.InputFrom, job.InputTo, from, to)
	}
}

func TestClient_ReLogsInOn401(t *testing.T) {
	api := newFakeAPI(t)
	client := newTestClient(t, api)
	ctx := context.Background()

	if err := client.Login(ctx); err != nil {
		t.Fatalf("Login: %v", err)
	}

	// Expire the cached token by issuing a new one out-of-band.
	api.tokenSeq.Add(1)

	from := time.Date(2026, 11, 1, 14, 0, 0, 0, time.UTC)
	job, err := client.CreateRevertJob(ctx, from, from.Add(10*time.Minute))
	if err != nil {
		t.Fatalf("CreateRevertJob after token expiry: %v", err)
	}
	if job == nil {
		t.Fatal("expected a job after transparent re-login")
	}
	if api.loginCalls.Load() != 2 {
		t.Errorf("login calls = %d, want 2 (initial + re-login on 401)", api.loginCalls.Load())
	}
}

func TestCreateRevertJob_ValidationErrorSurfaced(t *testing.T) {
	api := newFakeAPI(t)
	client := newTestClient(t, api)
	ctx := context.Background()

	if err := client.Login(ctx); err != nil {
		t.Fatalf("Login: %v", err)
	}

	now := time.Now()
	_, err := client.CreateRevertJob(ctx, now, now.Add(-time.Minute))
	if err == nil || !strings.Contains(err.Error(), "from must be before to") {
		t.Fatalf("error = %v, want it to contain %q", err, "from must be before to")
	}
}

func TestGetRevertJob_NotFound(t *testing.T) {
	api := newFakeAPI(t)
	client := newTestClient(t, api)
	ctx := context.Background()

	if err := client.Login(ctx); err != nil {
		t.Fatalf("Login: %v", err)
	}

	_, err := client.GetRevertJob(ctx, "99999999-9999-9999-9999-999999999999")
	if err == nil || !strings.Contains(err.Error(), "revert job not found") {
		t.Fatalf("error = %v, want it to contain %q", err, "revert job not found")
	}
}

func TestGetRevertJob_Success(t *testing.T) {
	api := newFakeAPI(t)
	client := newTestClient(t, api)
	ctx := context.Background()

	if err := client.Login(ctx); err != nil {
		t.Fatalf("Login: %v", err)
	}

	from := time.Date(2026, 11, 1, 14, 0, 0, 0, time.UTC)
	created, err := client.CreateRevertJob(ctx, from, from.Add(10*time.Minute))
	if err != nil {
		t.Fatalf("CreateRevertJob: %v", err)
	}

	job, err := client.GetRevertJob(ctx, created.ID.String())
	if err != nil {
		t.Fatalf("GetRevertJob: %v", err)
	}
	if job.Status != "pending" {
		t.Errorf("status = %q, want %q", job.Status, "pending")
	}
}

func TestPreviewRevert_Success(t *testing.T) {
	api := newFakeAPI(t)
	client := newTestClient(t, api)
	ctx := context.Background()

	if err := client.Login(ctx); err != nil {
		t.Fatalf("Login: %v", err)
	}

	from := time.Date(2026, 11, 1, 14, 0, 0, 0, time.UTC)
	preview, err := client.PreviewRevert(ctx, from, from.Add(10*time.Minute))
	if err != nil {
		t.Fatalf("PreviewRevert: %v", err)
	}
	if preview.Summary.Conflicts != 1 || preview.Summary.RowsAffected != 1 {
		t.Errorf("summary = %+v, want 1 conflict / 1 row affected", preview.Summary)
	}
	if len(preview.Tables) != 1 || preview.Tables[0].Table != "public.orders" {
		t.Fatalf("tables = %+v", preview.Tables)
	}
	rows := preview.Tables[0].Rows
	if rows[0].Effect != domain.PreviewEffectUpdate || rows[0].Reverted["status"] != "processing" {
		t.Errorf("row 0 = %+v", rows[0])
	}
	if rows[1].Effect != domain.PreviewEffectConflict || rows[1].Conflict.Constraint != "order_items_order_id_fkey" {
		t.Errorf("row 1 = %+v", rows[1])
	}
}

func TestPreviewRevert_InvalidRange(t *testing.T) {
	api := newFakeAPI(t)
	client := newTestClient(t, api)
	ctx := context.Background()

	if err := client.Login(ctx); err != nil {
		t.Fatalf("Login: %v", err)
	}

	now := time.Now()
	_, err := client.PreviewRevert(ctx, now, now.Add(-time.Minute))
	if err == nil || !strings.Contains(err.Error(), "from must be before to") {
		t.Fatalf("error = %v, want it to contain %q", err, "from must be before to")
	}
}
