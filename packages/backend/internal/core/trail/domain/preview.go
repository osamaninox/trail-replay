package domain

import "time"

// Revert preview: a dry run of a revert window. The executor applies every
// reverse DML inside a transaction on the source database, captures row
// states via RETURNING, and rolls back — so the preview shows each affected
// row's current state and its state after the revert, without persisting
// anything.

// RevertChange is one WAL change in a revert window, with the row payloads
// needed to locate and display the affected row.
type RevertChange struct {
	ID             uint64
	TransactionID  uint64
	ChangeSeqInTxn int32
	SchemaName     string
	TableName      string
	Op             string // original WAL op: I / U / D
	ChangedColumns []string
	OldRow         map[string]any
	NewRow         map[string]any
	ReverseDMLSQL  string
}

type PreviewEffect string

const (
	// PreviewEffectUpdate — row exists and will be updated back to old values.
	PreviewEffectUpdate PreviewEffect = "update"
	// PreviewEffectRestore — a deleted row will be re-inserted.
	PreviewEffectRestore PreviewEffect = "restore"
	// PreviewEffectDelete — an inserted row will be removed.
	PreviewEffectDelete PreviewEffect = "delete"
	// PreviewEffectNoop — the reverse statement would change nothing
	// (e.g. a re-insert that hits ON CONFLICT DO NOTHING).
	PreviewEffectNoop PreviewEffect = "noop"
	// PreviewEffectStale — the row is no longer in the state Trail Replay
	// recorded (someone changed or removed it after the window).
	PreviewEffectStale PreviewEffect = "stale"
	// PreviewEffectConflict — applying the reverse statement fails on a
	// database constraint; the revert would fail for this row.
	PreviewEffectConflict PreviewEffect = "conflict"
)

// PreviewRow is the per-row verdict: where the row is now, and where the
// revert leaves it.
type PreviewRow struct {
	Table          string           `json:"table"`
	PK             map[string]any   `json:"pk"`
	Effect         PreviewEffect    `json:"effect"`
	Current        map[string]any   `json:"current"`
	Reverted       map[string]any   `json:"reverted"`
	ChangedColumns []string         `json:"changed_columns,omitempty"`
	Transactions   []uint64         `json:"transactions"`
	Conflict       *PreviewConflict `json:"conflict,omitempty"`
	Warnings       []string         `json:"warnings,omitempty"`
}

type PreviewTable struct {
	Table string       `json:"table"`
	Rows  []PreviewRow `json:"rows"`
}

type PreviewSummary struct {
	Transactions   int `json:"transactions"`
	Changes        int `json:"changes"`
	RowsAffected   int `json:"rows_affected"`
	Conflicts      int `json:"conflicts"`
	Stale          int `json:"stale"`
	Noop           int `json:"noop"`
	SkippedChanges int `json:"skipped_changes"`
}

type RevertPreview struct {
	From    time.Time      `json:"from"`
	To      time.Time      `json:"to"`
	Summary PreviewSummary `json:"summary"`
	Tables  []PreviewTable `json:"tables"`
}

type PreviewRevertRequest struct {
	From time.Time `json:"from"`
	To   time.Time `json:"to"`
}

// PreviewConflict is a database error raised while dry-running one reverse
// statement (e.g. a FK violation on a re-delete).
type PreviewConflict struct {
	Code       string `json:"code"`
	Constraint string `json:"constraint,omitempty"`
	Detail     string `json:"detail,omitempty"`
	SQL        string `json:"sql"`
}
