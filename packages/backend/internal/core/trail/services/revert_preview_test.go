package services_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"trail-replay/internal/core/trail/domain"
	"trail-replay/internal/core/trail/ports/outbound"
	"trail-replay/internal/core/trail/services"
)

// fakePreviewExecutor captures the statements the service built and returns
// canned dry-run results.
type fakePreviewExecutor struct {
	got     []outbound.PreviewStatement
	results []outbound.PreviewStatementResult
	err     error
}

func (f *fakePreviewExecutor) Exec(ctx context.Context, sql string) error       { return nil }
func (f *fakePreviewExecutor) ExecTx(ctx context.Context, stmts []string) error { return nil }
func (f *fakePreviewExecutor) PreviewTx(ctx context.Context, stmts []outbound.PreviewStatement) ([]outbound.PreviewStatementResult, error) {
	f.got = stmts
	if f.err != nil {
		return nil, f.err
	}
	return f.results, nil
}

var (
	previewFrom = time.Date(2026, 11, 1, 14, 0, 0, 0, time.UTC)
	previewTo   = time.Date(2026, 11, 1, 14, 10, 0, 0, time.UTC)
)

func change(id, txnID uint64, seq int32, table, op string, oldRow, newRow map[string]any, reverseSQL string) domain.RevertChange {
	return domain.RevertChange{
		ID:             id,
		TransactionID:  txnID,
		ChangeSeqInTxn: seq,
		SchemaName:     "public",
		TableName:      table,
		Op:             op,
		OldRow:         oldRow,
		NewRow:         newRow,
		ReverseDMLSQL:  reverseSQL,
	}
}

func TestPreviewRevert_InvalidRange(t *testing.T) {
	repo := newMockRevertRepository()
	svc := services.NewRevertService(repo, &fakeSubmitter{}, &fakePreviewExecutor{})

	_, err := svc.PreviewRevert(context.Background(), previewTo, previewFrom)
	if err == nil {
		t.Fatal("expected error for from >= to")
	}
}

func TestPreviewRevert_NoExecutor(t *testing.T) {
	repo := newMockRevertRepository()
	svc := services.NewRevertService(repo, &fakeSubmitter{}, nil)

	_, err := svc.PreviewRevert(context.Background(), previewFrom, previewTo)
	if err == nil {
		t.Fatal("expected error when no source executor is configured")
	}
}

func TestPreviewRevert_BuildsStatementsSkipsEmpty(t *testing.T) {
	repo := newMockRevertRepository()
	repo.changes = []domain.RevertChange{
		change(1, 101, 2, "orders", "U", map[string]any{"id": "17", "status": "processing"}, map[string]any{"id": "17", "status": "paid"}, "UPDATE public.orders SET status = 'processing' WHERE id = '17'"),
		change(2, 101, 1, "orders", "U", map[string]any{"id": "17"}, nil, ""), // no REPLICA IDENTITY FULL
		change(3, 102, 1, "customers", "D", map[string]any{"id": "7", "name": "Ada"}, nil, "INSERT INTO public.customers ... ON CONFLICT DO NOTHING"),
	}
	exec := &fakePreviewExecutor{
		results: []outbound.PreviewStatementResult{
			{PK: map[string]any{"id": "17"}, Before: map[string]any{"id": "17", "status": "paid"}, Returned: []map[string]any{{"id": "17", "status": "processing"}}},
			{PK: map[string]any{"id": "7"}, Before: nil, Returned: []map[string]any{{"id": "7", "name": "Ada"}}},
		},
	}
	svc := services.NewRevertService(repo, &fakeSubmitter{}, exec)

	preview, err := svc.PreviewRevert(context.Background(), previewFrom, previewTo)
	if err != nil {
		t.Fatalf("PreviewRevert: %v", err)
	}

	if len(exec.got) != 2 {
		t.Fatalf("expected 2 statements (1 skipped), got %d", len(exec.got))
	}
	// For a DELETE change, the row locator must come from old_row.
	if exec.got[1].Row["name"] != "Ada" {
		t.Errorf("DELETE change should locate via old_row, got %v", exec.got[1].Row)
	}
	if preview.Summary.SkippedChanges != 1 {
		t.Errorf("skipped = %d, want 1", preview.Summary.SkippedChanges)
	}
	if preview.Summary.Changes != 3 {
		t.Errorf("changes = %d, want 3", preview.Summary.Changes)
	}
	if preview.Summary.Transactions != 2 {
		t.Errorf("transactions = %d, want 2 distinct", preview.Summary.Transactions)
	}
}

func TestPreviewRevert_UpdateRowCurrentAndReverted(t *testing.T) {
	repo := newMockRevertRepository()
	repo.changes = []domain.RevertChange{
		change(1, 101, 1, "orders", "U",
			map[string]any{"id": "17", "status": "processing", "amount": "99"},
			map[string]any{"id": "17", "status": "paid", "amount": "99"},
			"UPDATE public.orders SET status = 'processing' WHERE id = '17'"),
	}
	repo.changes[0].ChangedColumns = []string{"status"}
	exec := &fakePreviewExecutor{
		results: []outbound.PreviewStatementResult{{
			PK:       map[string]any{"id": "17"},
			Before:   map[string]any{"id": "17", "status": "paid", "amount": "99"},
			Returned: []map[string]any{{"id": "17", "status": "processing", "amount": "99"}},
		}},
	}
	svc := services.NewRevertService(repo, &fakeSubmitter{}, exec)

	preview, err := svc.PreviewRevert(context.Background(), previewFrom, previewTo)
	if err != nil {
		t.Fatalf("PreviewRevert: %v", err)
	}

	if len(preview.Tables) != 1 || preview.Tables[0].Table != "public.orders" {
		t.Fatalf("tables = %+v", preview.Tables)
	}
	row := preview.Tables[0].Rows[0]
	if row.Effect != domain.PreviewEffectUpdate {
		t.Errorf("effect = %s, want update", row.Effect)
	}
	if row.Current["status"] != "paid" {
		t.Errorf("current.status = %v, want paid", row.Current["status"])
	}
	if row.Reverted["status"] != "processing" {
		t.Errorf("reverted.status = %v, want processing", row.Reverted["status"])
	}
	if len(row.ChangedColumns) != 1 || row.ChangedColumns[0] != "status" {
		t.Errorf("changed_columns = %v", row.ChangedColumns)
	}
	if preview.Summary.RowsAffected != 1 {
		t.Errorf("rows_affected = %d, want 1", preview.Summary.RowsAffected)
	}
}

// The row-42 scenario: inserted in the window, then updated. The revert
// applies update-reverse then insert-reverse; net effect is delete.
func TestPreviewRevert_InsertThenUpdate_NetDelete(t *testing.T) {
	repo := newMockRevertRepository()
	// Revert order: newest first (commit_lsn DESC).
	repo.changes = []domain.RevertChange{
		change(1, 105, 1, "orders", "U",
			map[string]any{"id": "42", "status": "pending"}, map[string]any{"id": "42", "status": "paid"},
			"UPDATE public.orders SET status = 'pending' WHERE id = '42'"),
		change(2, 101, 1, "orders", "I",
			nil, map[string]any{"id": "42", "status": "pending", "amount": "99"},
			"DELETE FROM public.orders WHERE id = '42'"),
	}
	exec := &fakePreviewExecutor{
		results: []outbound.PreviewStatementResult{
			{PK: map[string]any{"id": "42"}, Before: map[string]any{"id": "42", "status": "paid", "amount": "99"}, Returned: []map[string]any{{"id": "42", "status": "pending", "amount": "99"}}},
			{PK: map[string]any{"id": "42"}, Before: map[string]any{"id": "42", "status": "paid", "amount": "99"}, Returned: []map[string]any{{"id": "42", "status": "pending", "amount": "99"}}},
		},
	}
	svc := services.NewRevertService(repo, &fakeSubmitter{}, exec)

	preview, err := svc.PreviewRevert(context.Background(), previewFrom, previewTo)
	if err != nil {
		t.Fatalf("PreviewRevert: %v", err)
	}

	if len(preview.Tables) != 1 || len(preview.Tables[0].Rows) != 1 {
		t.Fatalf("expected 1 aggregated row, got %+v", preview.Tables)
	}
	row := preview.Tables[0].Rows[0]
	if row.Effect != domain.PreviewEffectDelete {
		t.Errorf("effect = %s, want delete (net of insert+update)", row.Effect)
	}
	if row.Reverted != nil {
		t.Errorf("reverted = %v, want nil (row gone)", row.Reverted)
	}
	if row.Current["status"] != "paid" {
		t.Errorf("current.status = %v, want paid (state at preview time)", row.Current["status"])
	}
	if len(row.Transactions) != 2 {
		t.Errorf("transactions = %v, want both [105 101]", row.Transactions)
	}
}

func TestPreviewRevert_RestoreDeletedRow(t *testing.T) {
	repo := newMockRevertRepository()
	repo.changes = []domain.RevertChange{
		change(1, 103, 1, "customers", "D",
			map[string]any{"id": "7", "name": "Ada"}, nil,
			"INSERT INTO public.customers (id, name) VALUES ('7', 'Ada') ON CONFLICT DO NOTHING"),
	}
	exec := &fakePreviewExecutor{
		results: []outbound.PreviewStatementResult{{
			PK:       map[string]any{"id": "7"},
			Before:   nil, // deleted in the window, absent now
			Returned: []map[string]any{{"id": "7", "name": "Ada"}},
		}},
	}
	svc := services.NewRevertService(repo, &fakeSubmitter{}, exec)

	preview, err := svc.PreviewRevert(context.Background(), previewFrom, previewTo)
	if err != nil {
		t.Fatalf("PreviewRevert: %v", err)
	}

	row := preview.Tables[0].Rows[0]
	if row.Effect != domain.PreviewEffectRestore {
		t.Errorf("effect = %s, want restore", row.Effect)
	}
	if row.Current != nil {
		t.Errorf("current = %v, want nil", row.Current)
	}
	if row.Reverted["name"] != "Ada" {
		t.Errorf("reverted = %v", row.Reverted)
	}
}

func TestPreviewRevert_Conflict(t *testing.T) {
	repo := newMockRevertRepository()
	repo.changes = []domain.RevertChange{
		change(1, 101, 1, "orders", "I",
			nil, map[string]any{"id": "42"},
			"DELETE FROM public.orders WHERE id = '42'"),
	}
	exec := &fakePreviewExecutor{
		results: []outbound.PreviewStatementResult{{
			PK:     map[string]any{"id": "42"},
			Before: map[string]any{"id": "42", "status": "paid"},
			Conflict: &domain.PreviewConflict{
				Code:       "23503",
				Constraint: "order_items_order_id_fkey",
				Detail:     "Key (id)=(42) is still referenced from table \"order_items\".",
				SQL:        "DELETE FROM public.orders WHERE id = '42'",
			},
		}},
	}
	svc := services.NewRevertService(repo, &fakeSubmitter{}, exec)

	preview, err := svc.PreviewRevert(context.Background(), previewFrom, previewTo)
	if err != nil {
		t.Fatalf("PreviewRevert: %v", err)
	}

	row := preview.Tables[0].Rows[0]
	if row.Effect != domain.PreviewEffectConflict {
		t.Errorf("effect = %s, want conflict", row.Effect)
	}
	if row.Conflict == nil || row.Conflict.Constraint != "order_items_order_id_fkey" {
		t.Errorf("conflict = %+v", row.Conflict)
	}
	if preview.Summary.Conflicts != 1 || preview.Summary.RowsAffected != 0 {
		t.Errorf("summary = %+v, want conflicts=1 rows_affected=0", preview.Summary)
	}
}

func TestPreviewRevert_StaleAndNoop(t *testing.T) {
	repo := newMockRevertRepository()
	repo.changes = []domain.RevertChange{
		// Stale: the row was changed by someone else after the window.
		change(1, 102, 1, "orders", "U",
			map[string]any{"id": "17", "status": "processing"}, map[string]any{"id": "17", "status": "paid"},
			"UPDATE public.orders SET status = 'processing' WHERE id = '17'"),
		// Noop: the deleted row is already back (or conflicts elsewhere).
		change(2, 103, 1, "customers", "D",
			map[string]any{"id": "7", "name": "Ada"}, nil,
			"INSERT INTO public.customers (id, name) VALUES ('7', 'Ada') ON CONFLICT DO NOTHING"),
	}
	exec := &fakePreviewExecutor{
		results: []outbound.PreviewStatementResult{
			{PK: map[string]any{"id": "17"}, Before: nil, Returned: nil}, // row 17 gone entirely
			{PK: map[string]any{"id": "7"}, Before: map[string]any{"id": "7", "name": "Ada"}, Returned: nil},
		},
	}
	svc := services.NewRevertService(repo, &fakeSubmitter{}, exec)

	preview, err := svc.PreviewRevert(context.Background(), previewFrom, previewTo)
	if err != nil {
		t.Fatalf("PreviewRevert: %v", err)
	}

	var ordersRow, customersRow *domain.PreviewRow
	for ti := range preview.Tables {
		for ri := range preview.Tables[ti].Rows {
			switch preview.Tables[ti].Table {
			case "public.orders":
				ordersRow = &preview.Tables[ti].Rows[ri]
			case "public.customers":
				customersRow = &preview.Tables[ti].Rows[ri]
			}
		}
	}
	if ordersRow == nil || customersRow == nil {
		t.Fatalf("missing rows: %+v", preview.Tables)
	}
	if ordersRow.Effect != domain.PreviewEffectStale {
		t.Errorf("orders effect = %s, want stale", ordersRow.Effect)
	}
	if customersRow.Effect != domain.PreviewEffectUpdate {
		// Noop restore with the row already present reads as "update" to its
		// current content... verify the warning marks it as a no-op instead.
		if customersRow.Effect != domain.PreviewEffectNoop {
			t.Errorf("customers effect = %s, want noop (or update with noop warning)", customersRow.Effect)
		}
	}
	if len(customersRow.Warnings) != 1 || customersRow.Warnings[0] != "noop" {
		t.Errorf("customers warnings = %v, want [noop]", customersRow.Warnings)
	}
	if preview.Summary.Stale != 1 {
		t.Errorf("stale = %d, want 1", preview.Summary.Stale)
	}
}

func TestPreviewRevert_ExecutorError(t *testing.T) {
	repo := newMockRevertRepository()
	repo.changes = []domain.RevertChange{
		change(1, 101, 1, "orders", "I", nil, map[string]any{"id": "42"}, "DELETE FROM public.orders WHERE id = '42'"),
	}
	svc := services.NewRevertService(repo, &fakeSubmitter{}, &fakePreviewExecutor{err: errors.New("connection lost")})

	_, err := svc.PreviewRevert(context.Background(), previewFrom, previewTo)
	if err == nil {
		t.Fatal("expected executor error to propagate")
	}
}
