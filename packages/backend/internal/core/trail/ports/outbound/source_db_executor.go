package outbound

import (
	"context"

	"trail-replay/internal/core/trail/domain"
)

type SourceDBExecutor interface {
	Exec(ctx context.Context, sql string) error
	// ExecTx executes all statements atomically in a single transaction.
	ExecTx(ctx context.Context, stmts []string) error

	// PreviewTx dry-runs statements in order inside one transaction and rolls
	// back: for each statement it captures the affected row's state before the
	// run (looked up by primary key) and the rows produced by appending
	// "RETURNING *" to the statement. Statement failures are reported as
	// conflicts (the transaction continues from a savepoint); nothing is
	// committed.
	PreviewTx(ctx context.Context, stmts []PreviewStatement) ([]PreviewStatementResult, error)
}

// PreviewStatement is one reverse DML with enough context to locate the row
// it targets.
type PreviewStatement struct {
	SQL    string
	Schema string
	Table  string
	Op     string         // original WAL op (I/U/D) — the statement reverses it
	Row    map[string]any // row used to derive PK values (new_row for I/U, old_row for D)
}

// PreviewStatementResult is what the dry run observed for one statement.
// Before is the row's state at the start of the run (nil if the row does not
// exist or its PK could not be resolved). Returned holds the rows produced by
// the statement's RETURNING clause (empty when it matched nothing — stale
// update/delete or no-op insert).
type PreviewStatementResult struct {
	PK       map[string]any
	Before   map[string]any
	Returned []map[string]any
	Conflict *domain.PreviewConflict
}
