package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"

	"trail-replay/internal/core/trail/domain"
)

type createRevertJobInput struct {
	From time.Time `json:"from" jsonschema:"Start of the window to revert, RFC 3339 (e.g. 2026-11-01T14:02:00Z). Changes committed at or after this time are reverted."`
	To   time.Time `json:"to" jsonschema:"End of the window to revert, RFC 3339. Must be after 'from'. Changes committed at or before this time are reverted."`
}

type getRevertJobInput struct {
	JobID string `json:"job_id" jsonschema:"ID of the revert job, as returned by create_revert_job"`
}

type previewRevertInput struct {
	From time.Time `json:"from" jsonschema:"Start of the window to preview, RFC 3339 (e.g. 2026-11-01T14:02:00Z). Changes committed at or after this time would be reverted."`
	To   time.Time `json:"to" jsonschema:"End of the window to preview, RFC 3339. Must be after 'from'. Changes committed at or before this time would be reverted."`
}

// toolHandlers holds the dependencies shared by the MCP tool handlers.
type toolHandlers struct {
	client *Client
}

// previewRevert dry-runs the window on the source database (rolled back —
// nothing is applied) and renders each affected row's current → reverted
// state, so the user can approve a revert with full knowledge of the outcome.
func (h *toolHandlers) previewRevert(ctx context.Context, _ *mcpsdk.CallToolRequest, in previewRevertInput) (*mcpsdk.CallToolResult, any, error) {
	preview, err := h.client.PreviewRevert(ctx, in.From, in.To)
	if err != nil {
		return nil, nil, err
	}

	detail, err := json.MarshalIndent(preview, "", "  ")
	if err != nil {
		detail = []byte(fmt.Sprintf("(failed to marshal preview: %v)", err))
	}
	return &mcpsdk.CallToolResult{
		Content: []mcpsdk.Content{
			&mcpsdk.TextContent{Text: renderPreviewText(preview) + "\n\nFull preview JSON:\n" + string(detail)},
		},
	}, nil, nil
}

// renderPreviewText renders the preview as the agent-facing summary: counts
// first, then one line per affected row with column-level before → after.
func renderPreviewText(p *domain.RevertPreview) string {
	var b strings.Builder
	s := p.Summary
	fmt.Fprintf(&b, "Revert preview %s -> %s\n", p.From.Format(time.RFC3339), p.To.Format(time.RFC3339))
	fmt.Fprintf(&b, "%d transactions, %d changes, %d rows affected in %d table(s); %d conflict(s), %d stale, %d no-op, %d skipped.\n",
		s.Transactions, s.Changes, s.RowsAffected, len(p.Tables), s.Conflicts, s.Stale, s.Noop, s.SkippedChanges)

	for _, tbl := range p.Tables {
		fmt.Fprintf(&b, "\n%s (%d row(s)):\n", tbl.Table, len(tbl.Rows))
		for _, row := range tbl.Rows {
			fmt.Fprintf(&b, "  [%s] pk %s\n", row.Effect, formatPK(row.PK))
			switch row.Effect {
			case domain.PreviewEffectUpdate:
				for _, col := range row.ChangedColumns {
					fmt.Fprintf(&b, "    %s: %s -> %s\n", col, formatValue(row.Current[col]), formatValue(row.Reverted[col]))
				}
			case domain.PreviewEffectRestore:
				fmt.Fprintf(&b, "    row will be restored: %s\n", formatRow(row.Reverted))
			case domain.PreviewEffectDelete:
				fmt.Fprintf(&b, "    row will be removed; currently: %s\n", formatRow(row.Current))
			case domain.PreviewEffectConflict:
				fmt.Fprintf(&b, "    applying the revert fails: %s (%s) %s\n", row.Conflict.Code, row.Conflict.Constraint, row.Conflict.Detail)
			case domain.PreviewEffectStale:
				fmt.Fprintf(&b, "    row is no longer in the recorded state — revert would not apply\n")
			case domain.PreviewEffectNoop:
				fmt.Fprintf(&b, "    already in the reverted state — no change\n")
			}
		}
	}

	if s.Conflicts > 0 {
		fmt.Fprintf(&b, "\nWARNING: %d row(s) would fail to revert due to constraints.\n", s.Conflicts)
	}
	b.WriteString("\nNothing has been applied. To execute this revert, call create_revert_job with the same from/to window.")
	return b.String()
}

func formatPK(pk map[string]any) string {
	if len(pk) == 0 {
		return "(unresolved)"
	}
	keys := make([]string, 0, len(pk))
	for k := range pk {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s=%s", k, formatValue(pk[k])))
	}
	return strings.Join(parts, ", ")
}

func formatRow(row map[string]any) string {
	if row == nil {
		return "(row absent)"
	}
	keys := make([]string, 0, len(row))
	for k := range row {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s=%s", k, formatValue(row[k])))
	}
	return strings.Join(parts, ", ")
}

func formatValue(v any) string {
	switch t := v.(type) {
	case nil:
		return "NULL"
	case string:
		return t
	default:
		return fmt.Sprintf("%v", t)
	}
}

func (h *toolHandlers) createRevertJob(ctx context.Context, _ *mcpsdk.CallToolRequest, in createRevertJobInput) (*mcpsdk.CallToolResult, any, error) {
	job, err := h.client.CreateRevertJob(ctx, in.From, in.To)
	if err != nil {
		return nil, nil, err
	}

	summary := fmt.Sprintf(
		"Revert job %s created.\nWindow: %s -> %s\n%s\nPoll get_revert_job with job_id %q until status is completed, failed or cancelled.",
		job.ID, job.InputFrom.Format(time.RFC3339), job.InputTo.Format(time.RFC3339),
		progressLine(job), job.ID,
	)
	return textResult(summary, job), nil, nil
}

func (h *toolHandlers) getRevertJob(ctx context.Context, _ *mcpsdk.CallToolRequest, in getRevertJobInput) (*mcpsdk.CallToolResult, any, error) {
	if in.JobID == "" {
		return nil, nil, fmt.Errorf("job_id is required")
	}
	job, err := h.client.GetRevertJob(ctx, in.JobID)
	if err != nil {
		return nil, nil, err
	}

	summary := fmt.Sprintf("Revert job %s\n%s", job.ID, progressLine(job))
	if job.LastError != "" {
		summary += "\nlast_error: " + job.LastError
	}
	switch job.Status {
	case string(domain.RevertJobStatusCompleted):
		summary += "\nThe job is complete — no further polling needed."
	case string(domain.RevertJobStatusFailed), string(domain.RevertJobStatusCancelled):
		summary += "\nThe job is in a terminal state — no further polling needed."
	default:
		summary += "\nThe job is still running — poll again in a few seconds."
	}
	return textResult(summary, job), nil, nil
}

// progressLine renders the one-line status the model reads first, e.g.
// "status=in_progress — 12/37 changes reverted, 0 failed".
func progressLine(job *domain.RevertJobResponse) string {
	return fmt.Sprintf("status=%s — %d/%d changes reverted, %d failed",
		job.Status, job.CompletedCount, job.TotalChanges, job.FailedCount)
}

// textResult packs a human/agent-readable summary plus the full job JSON into
// the tool result content.
func textResult(summary string, job *domain.RevertJobResponse) *mcpsdk.CallToolResult {
	detail, err := json.MarshalIndent(job, "", "  ")
	if err != nil {
		detail = []byte(fmt.Sprintf("(failed to marshal job: %v)", err))
	}
	return &mcpsdk.CallToolResult{
		Content: []mcpsdk.Content{
			&mcpsdk.TextContent{Text: summary + "\n\nFull job JSON:\n" + string(detail)},
		},
	}
}
