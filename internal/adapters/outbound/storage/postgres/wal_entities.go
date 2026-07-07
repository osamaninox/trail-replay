package postgres

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"time"

	"github.com/lib/pq"
)

type JSONB map[string]interface{}

func (j JSONB) Value() (driver.Value, error) {
	if j == nil {
		return nil, nil
	}
	return json.Marshal(j)
}

func (j *JSONB) Scan(value interface{}) error {
	if value == nil {
		*j = nil
		return nil
	}

	var bytes []byte
	switch v := value.(type) {
	case []byte:
		bytes = v
	case string:
		bytes = []byte(v)
	default:
		return fmt.Errorf("cannot scan %T into JSONB", value)
	}

	return json.Unmarshal(bytes, j)
}

type WalTransaction struct {
	ID             uint64         `gorm:"primaryKey;autoIncrement"`
	SourceSlot     string         `gorm:"not null;uniqueIndex:idx_wal_txn_slot_lsn;uniqueIndex:idx_wal_txn_slot_xid_lsn"`
	SourceDb       string         `gorm:"not null"`
	Xid            int64          `gorm:"not null;uniqueIndex:idx_wal_txn_slot_xid_lsn"`
	BeginLSN       *string        `gorm:"type:pg_lsn"`
	CommitLSN      string         `gorm:"not null;type:pg_lsn;uniqueIndex:idx_wal_txn_slot_lsn;uniqueIndex:idx_wal_txn_slot_xid_lsn"`
	EndLSN         *string        `gorm:"type:pg_lsn"`
	CommitTS       time.Time      `gorm:"not null"`
	OriginName     *string
	TenantID       *string
	ChangeCount    int32          `gorm:"not null;default:0"`
	ForwardSQLText *string
	ReverseSQLText *string
	ForwardSQLHash []byte         `gorm:"type:bytea"`
	ReverseSQLHash []byte         `gorm:"type:bytea"`
	SafetyFlags    pq.StringArray `gorm:"type:text[];not null;default:'{}'"`
	IngestBatchID  *string        `gorm:"type:uuid"`
	IngestedAt     time.Time      `gorm:"not null;default:now()"`

	Changes []WalChange `gorm:"foreignKey:TransactionID"`
}

func (WalTransaction) TableName() string {
	return "wal_transaction"
}

type WalChange struct {
	ID                  uint64         `gorm:"primaryKey;autoIncrement"`
	TransactionID       uint64         `gorm:"not null;index;uniqueIndex:idx_wal_change_txn_seq"`
	ChangeSeqInTxn      int32          `gorm:"not null;uniqueIndex:idx_wal_change_txn_seq"`
	ChangeLSN           *string        `gorm:"type:pg_lsn"`
	TenantID            *string
	SchemaName          string         `gorm:"not null"`
	Table               string         `gorm:"not null;column:table_name"`
	TableOID            *uint32        `gorm:"type:oid"`
	Op                  string         `gorm:"not null;type:char(1)"`
	ReplicaIdentityMode *string
	RowPK               *JSONB         `gorm:"type:jsonb"`
	OldRow              *JSONB         `gorm:"type:jsonb"`
	NewRow              *JSONB         `gorm:"type:jsonb"`
	ChangedColumns      pq.StringArray `gorm:"type:text[];not null;default:'{}'"`
	ForwardDMLSQL       string         `gorm:"not null"`
	ReverseDMLSQL       string         `gorm:"not null"`
	ForwardSQLHash      []byte         `gorm:"type:bytea"`
	ReverseSQLHash      []byte         `gorm:"type:bytea"`
	ReverseWhereSQL     *string
	AffectedRowCount    int32          `gorm:"not null;default:1"`
	UndoStatus          string         `gorm:"not null;default:not_applied"`
	UndoAppliedAt       *time.Time
	UndoAppliedBy       *string
	SafetyFlags         pq.StringArray `gorm:"type:text[];not null;default:'{}'"`
	CreatedAt           time.Time      `gorm:"not null;default:now()"`
}

func (WalChange) TableName() string {
	return "wal_change"
}
