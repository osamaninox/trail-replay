package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	"trail-replay/internal/core/trail/domain"
	"trail-replay/internal/core/trail/ports/outbound"
)

type walQueryRepository struct {
	db *sqlx.DB
}

func NewWalQueryRepository(db *sqlx.DB) outbound.WalQueryRepository {
	return &walQueryRepository{db: db}
}

type walJoinRow struct {
	ID             uint64         `db:"id"`
	SourceSlot     string         `db:"source_slot"`
	SourceDb       string         `db:"source_db"`
	Xid            int64          `db:"xid"`
	CommitLSN      *string        `db:"commit_lsn"`
	CommitTS       string         `db:"commit_ts"`
	ChangeCount    int32          `db:"change_count"`
	IngestedAt     string         `db:"ingested_at"`
	ChangeID       *uint64        `db:"change_id"`
	ChangeSeqInTxn *int32         `db:"change_seq_in_txn"`
	SchemaName     *string        `db:"schema_name"`
	TableName      *string        `db:"table_name"`
	Op             *string        `db:"op"`
	ChangedColumns pq.StringArray `db:"changed_columns"`
	ForwardDMLSQL  *string        `db:"forward_dml_sql"`
	ReverseDMLSQL  *string        `db:"reverse_dml_sql"`
	UndoStatus     *string        `db:"undo_status"`
	CreatedAt      *string        `db:"created_at"`
}

func (r *walQueryRepository) ListWalTransactionsPaginated(ctx context.Context, page, pageSize int) (*domain.PaginatedResponse[domain.WalTransactionResult], error) {
	var totalCount int
	err := r.db.GetContext(ctx, &totalCount, "SELECT COUNT(*) FROM wal_transaction")
	if err != nil {
		return nil, fmt.Errorf("count wal transactions: %w", err)
	}

	if totalCount == 0 {
		return &domain.PaginatedResponse[domain.WalTransactionResult]{
			Data:       []domain.WalTransactionResult{},
			TotalCount: 0,
		}, nil
	}

	offset := (page - 1) * pageSize
	query := `
		SELECT
			t.id, t.source_slot, t.source_db, t.xid,
			t.commit_lsn::text AS commit_lsn,
			t.commit_ts::text AS commit_ts,
			t.change_count,
			t.ingested_at::text AS ingested_at,
			c.id AS change_id,
			c.change_seq_in_txn,
			c.schema_name,
			c.table_name,
			c.op,
			c.changed_columns,
			c.forward_dml_sql,
			c.reverse_dml_sql,
			c.undo_status,
			c.created_at::text AS created_at
		FROM (
			SELECT * FROM wal_transaction
			ORDER BY ingested_at DESC
			LIMIT $1 OFFSET $2
		) t
		LEFT JOIN wal_change c ON c.transaction_id = t.id
		ORDER BY t.ingested_at DESC, t.id, c.change_seq_in_txn`

	var rows []walJoinRow
	if err := r.db.SelectContext(ctx, &rows, query, pageSize, offset); err != nil {
		return nil, fmt.Errorf("query wal transactions: %w", err)
	}

	result := groupRows(rows)
	return &domain.PaginatedResponse[domain.WalTransactionResult]{
		Data:       result,
		TotalCount: totalCount,
	}, nil
}

func groupRows(rows []walJoinRow) []domain.WalTransactionResult {
	txnMap := make(map[uint64]*domain.WalTransactionResult)
	var ordered []uint64

	for _, row := range rows {
		txn, exists := txnMap[row.ID]
		if !exists {
			txn = &domain.WalTransactionResult{
				ID:          row.ID,
				SourceSlot:  row.SourceSlot,
				SourceDb:    row.SourceDb,
				Xid:         row.Xid,
				CommitLSN:   row.CommitLSN,
				ChangeCount: row.ChangeCount,
				Changes:     []domain.WalChangeResult{},
			}
			if ct, err := parseTime(row.CommitTS); err == nil {
				txn.CommitTS = ct
			}
			if ia, err := parseTime(row.IngestedAt); err == nil {
				txn.IngestedAt = ia
			}
			txnMap[row.ID] = txn
			ordered = append(ordered, row.ID)
		}

		if row.ChangeID != nil {
			change := domain.WalChangeResult{
				ID:             *row.ChangeID,
				TransactionID:  row.ID,
				ChangeSeqInTxn: *row.ChangeSeqInTxn,
				SchemaName:     *row.SchemaName,
				TableName:      *row.TableName,
				Op:             *row.Op,
				ForwardDMLSQL:  *row.ForwardDMLSQL,
				ReverseDMLSQL:  *row.ReverseDMLSQL,
				UndoStatus:     *row.UndoStatus,
			}
			if row.ChangedColumns != nil {
				change.ChangedColumns = []string(row.ChangedColumns)
			}
			if ca, err := parseTime(*row.CreatedAt); err == nil {
				change.CreatedAt = ca
			}
			txn.Changes = append(txn.Changes, change)
		}
	}

	result := make([]domain.WalTransactionResult, 0, len(ordered))
	for _, id := range ordered {
		if txn, ok := txnMap[id]; ok {
			result = append(result, *txn)
		}
	}
	return result
}

func parseTime(s string) (time.Time, error) {
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t, nil
	}
	return time.Time{}, fmt.Errorf("cannot parse time: %s", s)
}
