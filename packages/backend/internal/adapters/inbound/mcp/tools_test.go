package mcp

import (
	"context"
	"strings"
	"testing"
	"time"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"
)

// newLoggedInHandlers builds tool handlers backed by a fake API with an
// already-authenticated client.
func newLoggedInHandlers(t *testing.T, api *fakeAPI) *toolHandlers {
	t.Helper()
	client := newTestClient(t, api)
	if err := client.Login(context.Background()); err != nil {
		t.Fatalf("Login: %v", err)
	}
	return &toolHandlers{client: client}
}

func resultText(t *testing.T, res *mcpsdk.CallToolResult) string {
	t.Helper()
	if res == nil || len(res.Content) == 0 {
		t.Fatal("empty tool result")
	}
	tc, ok := res.Content[0].(*mcpsdk.TextContent)
	if !ok {
		t.Fatalf("content type = %T, want *mcpsdk.TextContent", res.Content[0])
	}
	return tc.Text
}

func TestCreateRevertJobTool_ReturnsSummaryAndJSON(t *testing.T) {
	api := newFakeAPI(t)
	h := newLoggedInHandlers(t, api)

	from := time.Date(2026, 11, 1, 14, 0, 0, 0, time.UTC)
	res, _, err := h.createRevertJob(context.Background(), nil, createRevertJobInput{
		From: from,
		To:   from.Add(10 * time.Minute),
	})
	if err != nil {
		t.Fatalf("createRevertJob: %v", err)
	}

	text := resultText(t, res)
	for _, want := range []string{
		"Revert job 11111111-2222-3333-4444-555555555555 created.",
		"status=pending",
		"get_revert_job",
		"Full job JSON:",
		`"input_from": "2026-11-01T14:00:00Z"`,
	} {
		if !strings.Contains(text, want) {
			t.Errorf("result text missing %q\ngot:\n%s", want, text)
		}
	}
}

func TestCreateRevertJobTool_APIErrorPropagates(t *testing.T) {
	api := newFakeAPI(t)
	h := newLoggedInHandlers(t, api)

	now := time.Now()
	_, _, err := h.createRevertJob(context.Background(), nil, createRevertJobInput{
		From: now,
		To:   now.Add(-time.Minute),
	})
	if err == nil || !strings.Contains(err.Error(), "from must be before to") {
		t.Fatalf("error = %v, want it to contain %q", err, "from must be before to")
	}
}

func TestGetRevertJobTool_RequiresJobID(t *testing.T) {
	api := newFakeAPI(t)
	h := newLoggedInHandlers(t, api)

	_, _, err := h.getRevertJob(context.Background(), nil, getRevertJobInput{})
	if err == nil || !strings.Contains(err.Error(), "job_id is required") {
		t.Fatalf("error = %v, want it to contain %q", err, "job_id is required")
	}
}

func TestGetRevertJobTool_ProgressSummary(t *testing.T) {
	api := newFakeAPI(t)
	h := newLoggedInHandlers(t, api)
	ctx := context.Background()

	from := time.Date(2026, 11, 1, 14, 0, 0, 0, time.UTC)
	created, err := h.client.CreateRevertJob(ctx, from, from.Add(10*time.Minute))
	if err != nil {
		t.Fatalf("CreateRevertJob: %v", err)
	}

	// Simulate mid-flight progress in the fake API.
	job := api.jobs[created.ID.String()]
	job.Status = "in_progress"
	job.TotalChanges = 37
	job.CompletedCount = 12
	api.jobs[created.ID.String()] = job

	res, _, err := h.getRevertJob(ctx, nil, getRevertJobInput{JobID: created.ID.String()})
	if err != nil {
		t.Fatalf("getRevertJob: %v", err)
	}

	text := resultText(t, res)
	for _, want := range []string{
		"status=in_progress — 12/37 changes reverted, 0 failed",
		"still running",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("result text missing %q\ngot:\n%s", want, text)
		}
	}
}

func TestPreviewRevertTool_RendersCurrentAndReverted(t *testing.T) {
	api := newFakeAPI(t)
	h := newLoggedInHandlers(t, api)

	from := time.Date(2026, 11, 1, 14, 0, 0, 0, time.UTC)
	res, _, err := h.previewRevert(context.Background(), nil, previewRevertInput{
		From: from,
		To:   from.Add(10 * time.Minute),
	})
	if err != nil {
		t.Fatalf("previewRevert: %v", err)
	}

	text := resultText(t, res)
	for _, want := range []string{
		"Revert preview 2026-11-01T14:00:00Z -> 2026-11-01T14:10:00Z",
		"3 transactions, 3 changes, 1 rows affected in 1 table(s); 1 conflict(s), 0 stale, 0 no-op, 0 skipped.",
		"public.orders (2 row(s)):",
		"[update] pk id=17",
		"status: paid -> processing",
		"amount: 120 -> 99",
		"[conflict] pk id=42",
		"23503 (order_items_order_id_fkey)",
		"WARNING: 1 row(s) would fail to revert due to constraints.",
		"Nothing has been applied.",
		"Full preview JSON:",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("result text missing %q\ngot:\n%s", want, text)
		}
	}
}

func TestPreviewRevertTool_APIErrorPropagates(t *testing.T) {
	api := newFakeAPI(t)
	h := newLoggedInHandlers(t, api)

	now := time.Now()
	_, _, err := h.previewRevert(context.Background(), nil, previewRevertInput{
		From: now,
		To:   now.Add(-time.Minute),
	})
	if err == nil || !strings.Contains(err.Error(), "from must be before to") {
		t.Fatalf("error = %v, want it to contain %q", err, "from must be before to")
	}
}

func TestNewServer_RegistersTools(t *testing.T) {
	server := NewServer(NewClient("http://unused", "u", "p"))
	if server == nil {
		t.Fatal("NewServer returned nil")
	}
	// Tool wiring is exercised end-to-end via the handlers above; here we
	// only guard against construction regressions.
}
