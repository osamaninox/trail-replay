package postgres_test

import (
	"context"
	"fmt"
	"testing"

	"trail-replay/internal/adapters/outbound/storage/postgres"
	"trail-replay/internal/core/trail/ports/outbound"
)

// setupSourceTables creates the "demo app" schema the preview dry-runs against:
// orders(id PK) with a FK from order_items.
func setupSourceTables(t *testing.T, exec interface {
	Exec(ctx context.Context, sql string) error
}) {
	t.Helper()
	ctx := context.Background()
	for _, stmt := range []string{
		`DROP TABLE IF EXISTS order_items, orders`,
		`CREATE TABLE orders (id int primary key, status text, amount int)`,
		`CREATE TABLE order_items (id int primary key, order_id int not null references orders(id))`,
		`INSERT INTO orders VALUES (17, 'paid', 120), (42, 'paid', 99)`,
		`INSERT INTO order_items VALUES (1, 42)`,
	} {
		if err := exec.Exec(ctx, stmt); err != nil {
			t.Fatalf("setup %q: %v", stmt, err)
		}
	}
}

func TestPreviewTx_CapturesBeforeAfterConflictsAndRollsBack(t *testing.T) {
	ctx := context.Background()
	pgContainer, db := startPGWithRevertTables(ctx, t)
	defer func() { pgContainer.Terminate(ctx) }()
	defer db.Close()

	exec := postgres.NewSourceDBExecutor(db)
	setupSourceTables(t, exec)

	stmts := []outbound.PreviewStatement{
		{ // 0: reverse of an UPDATE — row present, will change
			SQL:    `UPDATE public.orders SET status = 'processing' WHERE id = '17'`,
			Schema: "public", Table: "orders", Op: "U",
			Row: map[string]any{"id": "17"},
		},
		{ // 1: reverse of an INSERT — blocked by the order_items FK
			SQL:    `DELETE FROM public.orders WHERE id = '42'`,
			Schema: "public", Table: "orders", Op: "I",
			Row: map[string]any{"id": "42"},
		},
		{ // 2: reverse of a DELETE — restores a row
			SQL:    `INSERT INTO public.orders (id, status, amount) VALUES ('7', 'new', 5) ON CONFLICT DO NOTHING`,
			Schema: "public", Table: "orders", Op: "D",
			Row: map[string]any{"id": "7"},
		},
		{ // 3: reverse of a DELETE whose row is already back — no-op
			SQL:    `INSERT INTO public.orders (id, status, amount) VALUES ('17', 'processing', 99) ON CONFLICT DO NOTHING`,
			Schema: "public", Table: "orders", Op: "D",
			Row: map[string]any{"id": "17"},
		},
		{ // 4: reverse of an UPDATE for a row that no longer exists — stale
			SQL:    `UPDATE public.orders SET status = 'x' WHERE id = '999'`,
			Schema: "public", Table: "orders", Op: "U",
			Row: map[string]any{"id": "999"},
		},
	}

	results, err := exec.PreviewTx(ctx, stmts)
	if err != nil {
		t.Fatalf("PreviewTx: %v", err)
	}
	if len(results) != len(stmts) {
		t.Fatalf("results = %d, want %d", len(results), len(stmts))
	}

	// 0: update — before/after captured
	if got := fmt.Sprint(results[0].Before["status"]); got != "paid" {
		t.Errorf("stmt 0 before.status = %v, want paid", got)
	}
	if len(results[0].Returned) != 1 || fmt.Sprint(results[0].Returned[0]["status"]) != "processing" {
		t.Errorf("stmt 0 returned = %+v, want status=processing", results[0].Returned)
	}
	if fmt.Sprint(results[0].PK["id"]) != "17" {
		t.Errorf("stmt 0 pk = %+v", results[0].PK)
	}

	// 1: FK conflict — reported, and the run continued past it
	if results[1].Conflict == nil {
		t.Fatal("stmt 1: expected conflict")
	}
	if results[1].Conflict.Code != "23503" || results[1].Conflict.Constraint != "order_items_order_id_fkey" {
		t.Errorf("stmt 1 conflict = %+v", results[1].Conflict)
	}

	// 2: restore — RETURNING yields the inserted row
	if len(results[2].Returned) != 1 || fmt.Sprint(results[2].Returned[0]["id"]) != "7" {
		t.Errorf("stmt 2 returned = %+v, want restored row id=7", results[2].Returned)
	}
	if results[2].Before != nil {
		t.Errorf("stmt 2 before = %+v, want nil (row absent)", results[2].Before)
	}

	// 3: no-op insert — RETURNING empty
	if len(results[3].Returned) != 0 {
		t.Errorf("stmt 3 returned = %+v, want empty (noop)", results[3].Returned)
	}

	// 4: stale update — nothing matched, no current row
	if len(results[4].Returned) != 0 || results[4].Before != nil {
		t.Errorf("stmt 4 = %+v, want empty returned and nil before", results[4])
	}

	// Rollback proof: the source tables are untouched.
	var status string
	if err := db.GetContext(ctx, &status, `SELECT status FROM orders WHERE id = 17`); err != nil {
		t.Fatal(err)
	}
	if status != "paid" {
		t.Errorf("orders 17 status = %q after preview, want %q (rollback)", status, "paid")
	}
	var count int
	if err := db.GetContext(ctx, &count, `SELECT COUNT(*) FROM orders`); err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Errorf("orders count = %d after preview, want 2 (rollback)", count)
	}
}

func TestPreviewTx_Empty(t *testing.T) {
	ctx := context.Background()
	pgContainer, db := startPGWithRevertTables(ctx, t)
	defer func() { pgContainer.Terminate(ctx) }()
	defer db.Close()

	exec := postgres.NewSourceDBExecutor(db)
	results, err := exec.PreviewTx(ctx, nil)
	if err != nil {
		t.Fatalf("PreviewTx(nil): %v", err)
	}
	if len(results) != 0 {
		t.Errorf("results = %d, want 0", len(results))
	}
}
