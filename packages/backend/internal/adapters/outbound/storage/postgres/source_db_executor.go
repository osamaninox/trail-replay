package postgres

import (
	"context"

	"github.com/jmoiron/sqlx"

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
