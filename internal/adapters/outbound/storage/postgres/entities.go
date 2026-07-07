package postgres

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"time"

	"github.com/lib/pq"
	"github.com/osamakhalid/trail-replay/internal/core/trail/domain"
)

// DomainEntity represents any entity that can be converted to/from domain models
type DomainEntity[T any] interface {
	ToDomain() T
}

// EntityConverter provides generic conversion methods
type EntityConverter[E DomainEntity[T], T any] struct{}

func (c EntityConverter[E, T]) ToDomainSlice(entities []E) []T {
	result := make([]T, len(entities))
	for i, entity := range entities {
		result[i] = entity.ToDomain()
	}
	return result
}

type TrailEntity struct {
	ID        string    `db:"id"`
	Name      string    `db:"name"`
	CreatedAt time.Time `db:"created_at"`
	UpdatedAt time.Time `db:"updated_at"`
}

type EventEntity struct {
	ID        string      `db:"id"`
	TrailID   string      `db:"trail_id"`
	Type      string      `db:"type"`
	Payload   PayloadJSON `db:"payload"`
	OccuredAt time.Time   `db:"occured_at"`
	Sequence  int64       `db:"sequence"`
}

type PayloadJSON map[string]any

func (p PayloadJSON) Value() (driver.Value, error) {
	if p == nil {
		return nil, nil
	}
	return json.Marshal(p)
}

func (p *PayloadJSON) Scan(value any) error {
	if value == nil {
		*p = nil
		return nil
	}

	var bytes []byte
	switch v := value.(type) {
	case []byte:
		bytes = v
	case string:
		bytes = []byte(v)
	default:
		return fmt.Errorf("cannot scan %T into PayloadJSON", value)
	}

	return json.Unmarshal(bytes, p)
}

func (e *TrailEntity) ToDomain() *domain.Trail {
	return &domain.Trail{
		ID:        e.ID,
		Name:      e.Name,
		Events:    []domain.Event{}, // Will be loaded separately
		CreatedAt: e.CreatedAt,
		UpdatedAt: e.UpdatedAt,
	}
}

func (e *EventEntity) ToDomain() domain.Event {
	return domain.Event{
		ID:        e.ID,
		TrailID:   e.TrailID,
		Type:      domain.EventType(e.Type),
		Payload:   map[string]any(e.Payload),
		OccuredAt: e.OccuredAt,
		Sequence:  e.Sequence,
	}
}

func TrailToEntity(t *domain.Trail) *TrailEntity {
	return &TrailEntity{
		ID:        t.ID,
		Name:      t.Name,
		CreatedAt: t.CreatedAt,
		UpdatedAt: t.UpdatedAt,
	}
}

func EventToEntity(e *domain.Event) *EventEntity {
	return &EventEntity{
		ID:        e.ID,
		TrailID:   e.TrailID,
		Type:      string(e.Type),
		Payload:   PayloadJSON(e.Payload),
		OccuredAt: e.OccuredAt,
		Sequence:  e.Sequence,
	}
}

// WalTransactionEntity maps to the wal_transaction table for CDC log persistence.
type WalTransactionEntity struct {
	ID             uint64         `db:"id"`
	SourceSlot     string         `db:"source_slot"`
	SourceDb       string         `db:"source_db"`
	Xid            int64          `db:"xid"`
	BeginLSN       *string        `db:"begin_lsn"`
	CommitLSN      string         `db:"commit_lsn"`
	EndLSN         *string        `db:"end_lsn"`
	CommitTS       time.Time      `db:"commit_ts"`
	OriginName     *string        `db:"origin_name"`
	TenantID       *string        `db:"tenant_id"`
	ChangeCount    int32          `db:"change_count"`
	ForwardSQLText *string        `db:"forward_sql_text"`
	ReverseSQLText *string        `db:"reverse_sql_text"`
	ForwardSQLHash []byte         `db:"forward_sql_hash"`
	ReverseSQLHash []byte         `db:"reverse_sql_hash"`
	SafetyFlags    pq.StringArray `db:"safety_flags"`
	IngestBatchID  *string        `db:"ingest_batch_id"`
	IngestedAt     time.Time      `db:"ingested_at"`
}

// WalChangeEntity maps to the wal_change table for individual row-level CDC changes.
type WalChangeEntity struct {
	ID                  uint64         `db:"id"`
	TransactionID       uint64         `db:"transaction_id"`
	ChangeSeqInTxn      int32          `db:"change_seq_in_txn"`
	ChangeLSN           *string        `db:"change_lsn"`
	TenantID            *string        `db:"tenant_id"`
	SchemaName          string         `db:"schema_name"`
	TableName           string         `db:"table_name"`
	TableOID            *uint32        `db:"table_oid"`
	Op                  string         `db:"op"`
	ReplicaIdentityMode *string        `db:"replica_identity_mode"`
	RowPK               *PayloadJSON   `db:"row_pk"`
	OldRow              *PayloadJSON   `db:"old_row"`
	NewRow              *PayloadJSON   `db:"new_row"`
	ChangedColumns      pq.StringArray `db:"changed_columns"`
	ForwardDMLSQL       string         `db:"forward_dml_sql"`
	ReverseDMLSQL       string         `db:"reverse_dml_sql"`
	ForwardSQLHash      []byte         `db:"forward_sql_hash"`
	ReverseSQLHash      []byte         `db:"reverse_sql_hash"`
	ReverseWhereSQL     *string        `db:"reverse_where_sql"`
	AffectedRowCount    int32          `db:"affected_row_count"`
	UndoStatus          string         `db:"undo_status"`
	UndoAppliedAt       *time.Time     `db:"undo_applied_at"`
	UndoAppliedBy       *string        `db:"undo_applied_by"`
	SafetyFlags         pq.StringArray `db:"safety_flags"`
	CreatedAt           time.Time      `db:"created_at"`
}