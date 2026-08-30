package outbound

import "context"

type SourceDBExecutor interface {
	Exec(ctx context.Context, sql string) error
}
