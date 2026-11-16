package mcp

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"

	"trail-replay/internal/core/trail/domain"
)

// fakeAPI is a minimal in-memory stand-in for the Trail Replay REST API.
// It only accepts the most recently issued token, so the client's 401
// re-login path is exercised realistically.
type fakeAPI struct {
	t          *testing.T
	loginCalls atomic.Int32
	tokenSeq   atomic.Int32

	validEmail    string
	validPassword string

	jobs map[string]domain.RevertJobResponse
}

func newFakeAPI(t *testing.T) *fakeAPI {
	t.Helper()
	return &fakeAPI{
		t:             t,
		validEmail:    "demo@example.com",
		validPassword: "secret",
		jobs:          map[string]domain.RevertJobResponse{},
	}
}

func (f *fakeAPI) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /auth/login", f.handleLogin)
	mux.HandleFunc("POST /reverts/preview", f.handlePreview)
	mux.HandleFunc("POST /reverts/jobs", f.handleCreateJob)
	mux.HandleFunc("GET /reverts/jobs/{id}", f.handleGetJob)
	return mux
}

func (f *fakeAPI) handleLogin(w http.ResponseWriter, r *http.Request) {
	f.loginCalls.Add(1)
	var body struct {
		Email    string `json:"email"`
		Password string `json:"password"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if body.Email != f.validEmail || body.Password != f.validPassword {
		writeErr(w, http.StatusUnauthorized, "invalid credentials")
		return
	}
	writeJSON(f.t, w, http.StatusOK, map[string]any{
		"token": fmt.Sprintf("tok-%d", f.tokenSeq.Add(1)),
		"user":  map[string]string{"id": uuid.NewString(), "email": body.Email},
	})
}

// checkAuth accepts only the most recently issued token.
func (f *fakeAPI) checkAuth(w http.ResponseWriter, r *http.Request) bool {
	want := fmt.Sprintf("Bearer tok-%d", f.tokenSeq.Load())
	if f.tokenSeq.Load() == 0 || r.Header.Get("Authorization") != want {
		writeErr(w, http.StatusUnauthorized, "invalid or expired token")
		return false
	}
	return true
}

func (f *fakeAPI) handleCreateJob(w http.ResponseWriter, r *http.Request) {
	if !f.checkAuth(w, r) {
		return
	}
	var req domain.CreateRevertJobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if !req.From.Before(req.To) {
		writeErr(w, http.StatusBadRequest, "from must be before to")
		return
	}
	job := domain.RevertJobResponse{
		ID:        uuid.MustParse("11111111-2222-3333-4444-555555555555"),
		Status:    string(domain.RevertJobStatusPending),
		InputFrom: req.From,
		InputTo:   req.To,
	}
	f.jobs[job.ID.String()] = job
	writeJSON(f.t, w, http.StatusCreated, job)
}

// handlePreview returns a canned preview: one updated row and one
// delete-blocked-by-FK row in public.orders.
func (f *fakeAPI) handlePreview(w http.ResponseWriter, r *http.Request) {
	if !f.checkAuth(w, r) {
		return
	}
	var req domain.PreviewRevertRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if !req.From.Before(req.To) {
		writeErr(w, http.StatusBadRequest, "from must be before to")
		return
	}
	writeJSON(f.t, w, http.StatusOK, map[string]any{
		"from": req.From,
		"to":   req.To,
		"summary": map[string]any{
			"transactions": 3, "changes": 3, "rows_affected": 1,
			"conflicts": 1, "stale": 0, "noop": 0, "skipped_changes": 0,
		},
		"tables": []any{map[string]any{
			"table": "public.orders",
			"rows": []any{
				map[string]any{
					"table": "public.orders", "pk": map[string]any{"id": "17"}, "effect": "update",
					"current":         map[string]any{"id": "17", "status": "paid", "amount": "120"},
					"reverted":        map[string]any{"id": "17", "status": "processing", "amount": "99"},
					"changed_columns": []string{"status", "amount"},
					"transactions":    []uint64{102},
				},
				map[string]any{
					"table": "public.orders", "pk": map[string]any{"id": "42"}, "effect": "conflict",
					"current": map[string]any{"id": "42", "status": "paid"}, "reverted": nil,
					"transactions": []uint64{101},
					"conflict": map[string]any{
						"code": "23503", "constraint": "order_items_order_id_fkey",
						"detail": "Key (id)=(42) is still referenced from table \"order_items\".",
						"sql":    "DELETE FROM public.orders WHERE id = '42'",
					},
				},
			},
		}},
	})
}

func (f *fakeAPI) handleGetJob(w http.ResponseWriter, r *http.Request) {
	if !f.checkAuth(w, r) {
		return
	}
	job, ok := f.jobs[r.PathValue("id")]
	if !ok {
		writeErr(w, http.StatusNotFound, "revert job not found")
		return
	}
	writeJSON(f.t, w, http.StatusOK, job)
}

func writeErr(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

func writeJSON(t *testing.T, w http.ResponseWriter, status int, v any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		t.Fatalf("encode response: %v", err)
	}
}

// newTestClient returns a client pointed at a running fake API.
func newTestClient(t *testing.T, api *fakeAPI) *Client {
	t.Helper()
	srv := httptest.NewServer(api.handler())
	t.Cleanup(srv.Close)
	return NewClient(srv.URL, api.validEmail, api.validPassword)
}
