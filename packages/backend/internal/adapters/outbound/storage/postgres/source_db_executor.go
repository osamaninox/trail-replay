package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"

	"trail-replay/internal/core/trail/domain"
	"trail-replay/internal/core/trail/ports/outbound"
)

type sourceDBExecutor struct {
	db *sqlx.DB
}

func NewSourceDBExecutor(db *sqlx.DB) outbound.SourceDBExecutor {
	return &sourceDBExecutor{db: db}
}

func (e *sourceDBExecutor) Exec(ctx context.Context, sql string) error {
	_, err := e.db.ExecContext(ctx, sql)
	return err
}

// ExecTx executes all statements in one transaction so a transaction's
// reverse DMLs apply atomically, matching how the original changes committed.
func (e *sourceDBExecutor) ExecTx(ctx context.Context, stmts []string) error {
	tx, err := e.db.BeginTxx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() // no-op after Commit

	for _, stmt := range stmts {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// PreviewTx dry-runs the plan: inside one transaction it snapshots each
// target row's current state, replays every statement in order with
// "RETURNING *" appended, and rolls back. A failed statement is rolled back
// to its savepoint and reported as a conflict so later statements still
// preview; any non-Postgres error (connection loss etc.) aborts the run.
func (e *sourceDBExecutor) PreviewTx(ctx context.Context, stmts []outbound.PreviewStatement) ([]outbound.PreviewStatementResult, error) {
	results := make([]outbound.PreviewStatementResult, len(stmts))
	if len(stmts) == 0 {
		return results, nil
	}

	tx, err := e.db.BeginTxx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback() // the preview never commits

	pkCache := map[string][]string{}
	pkColumns := func(schema, table string) ([]string, error) {
		key := schema + "." + table
		if cols, ok := pkCache[key]; ok {
			return cols, nil
		}
		var cols []string
		err := tx.SelectContext(ctx, &cols, `
			SELECT kcu.column_name
			FROM information_schema.table_constraints tc
			JOIN information_schema.key_column_usage kcu
				ON tc.constraint_name = kcu.constraint_name
				AND tc.table_schema = kcu.table_schema
			WHERE tc.constraint_type = 'PRIMARY KEY'
				AND tc.table_schema = $1
				AND tc.table_name = $2
			ORDER BY kcu.ordinal_position`, schema, table)
		if err != nil {
			return nil, err
		}
		pkCache[key] = cols
		return cols, nil
	}

	// Pass 1: snapshot current row state (before any reverse statement runs).
	for i, st := range stmts {
		pkCols, err := pkColumns(st.Schema, st.Table)
		if err != nil {
			return nil, fmt.Errorf("resolve PK for %s.%s: %w", st.Schema, st.Table, err)
		}
		pk := extractPK(pkCols, st.Row)
		results[i].PK = pk
		if len(pkCols) == 0 || pk == nil {
			continue
		}
		before, err := selectRowByPK(ctx, tx, st.Schema, st.Table, pkCols, pk)
		if err != nil {
			return nil, fmt.Errorf("snapshot %s.%s: %w", st.Schema, st.Table, err)
		}
		results[i].Before = before
	}

	// Pass 2: replay each reverse statement, capturing RETURNING rows.
	for i, st := range stmts {
		sp := fmt.Sprintf("tr_preview_%d", i)
		if _, err := tx.ExecContext(ctx, "SAVEPOINT "+sp); err != nil {
			return nil, err
		}

		rows, err := queryReturning(ctx, tx, st.SQL+" RETURNING *")
		if err != nil {
			var pqErr *pq.Error
			if !errors.As(err, &pqErr) {
				return nil, fmt.Errorf("preview statement %d: %w", i, err)
			}
			if _, rbErr := tx.ExecContext(ctx, "ROLLBACK TO SAVEPOINT "+sp); rbErr != nil {
				return nil, fmt.Errorf("rollback to savepoint: %w", rbErr)
			}
			results[i].Conflict = &domain.PreviewConflict{
				Code:       string(pqErr.Code),
				Constraint: pqErr.Constraint,
				Detail:     pqErr.Detail,
				SQL:        st.SQL,
			}
			continue
		}
		if _, err := tx.ExecContext(ctx, "RELEASE SAVEPOINT "+sp); err != nil {
			return nil, err
		}
		results[i].Returned = rows
	}

	return results, nil
}

// extractPK pulls PK values out of a WAL row payload. Returns nil when any PK
// column is missing or null — the row then previews without a "current"
// snapshot (PK columns are never NULL in Postgres, so a null here means the
// payload lacks the column, e.g. no REPLICA IDENTITY FULL).
func extractPK(pkCols []string, row map[string]any) map[string]any {
	if len(pkCols) == 0 || row == nil {
		return nil
	}
	pk := make(map[string]any, len(pkCols))
	for _, col := range pkCols {
		v, ok := row[col]
		if !ok || v == nil {
			return nil
		}
		pk[col] = v
	}
	return pk
}

func selectRowByPK(ctx context.Context, tx *sqlx.Tx, schema, table string, pkCols []string, pk map[string]any) (map[string]any, error) {
	where := make([]string, 0, len(pkCols))
	args := make([]any, 0, len(pkCols))
	for i, col := range pkCols {
		where = append(where, fmt.Sprintf("%s = $%d", quoteIdent(col), i+1))
		args = append(args, pk[col])
	}
	query := fmt.Sprintf("SELECT * FROM %s.%s WHERE %s",
		quoteIdent(schema), quoteIdent(table), strings.Join(where, " AND "))

	rows, err := queryReturning(ctx, tx, query, args...)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}
	return rows[0], nil
}

// queryReturning runs a query and scans every row into a normalized map.
func queryReturning(ctx context.Context, tx *sqlx.Tx, query string, args ...any) ([]map[string]any, error) {
	rows, err := tx.QueryxContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []map[string]any
	for rows.Next() {
		m := map[string]any{}
		if err := rows.MapScan(m); err != nil {
			return nil, err
		}
		for k, v := range m {
			m[k] = normalizeValue(v)
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

// quoteIdent quotes a Postgres identifier. Inputs here are identifiers we
// resolved from the source database's own catalog (PK columns) or recorded
// from its WAL (schema/table names), never free-form user input.
func quoteIdent(ident string) string {
	return `"` + strings.ReplaceAll(ident, `"`, `""`) + `"`
}

// normalizeValue makes driver values JSON-friendly: []byte from text columns
// becomes string, timestamps become RFC 3339.
func normalizeValue(v any) any {
	switch t := v.(type) {
	case []byte:
		return string(t)
	case time.Time:
		return t.Format(time.RFC3339Nano)
	default:
		return v
	}
}
