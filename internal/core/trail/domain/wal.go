package domain

import "time"

type WalTransactionResult struct {
	ID          uint64            `json:"id"`
	SourceSlot  string            `json:"source_slot"`
	SourceDb    string            `json:"source_db"`
	Xid         int64             `json:"xid"`
	CommitLSN   *string           `json:"commit_lsn,omitempty"`
	CommitTS    time.Time         `json:"commit_ts"`
	ChangeCount int32             `json:"change_count"`
	IngestedAt  time.Time         `json:"ingested_at"`
	Changes     []WalChangeResult `json:"changes"`
}

type WalChangeResult struct {
	ID             uint64    `json:"id"`
	TransactionID  uint64    `json:"transaction_id"`
	ChangeSeqInTxn int32     `json:"change_seq_in_txn"`
	SchemaName     string    `json:"schema_name"`
	TableName      string    `json:"table_name"`
	Op             string    `json:"op"`
	ChangedColumns []string  `json:"changed_columns"`
	ForwardDMLSQL  string    `json:"forward_dml_sql"`
	ReverseDMLSQL  string    `json:"reverse_dml_sql"`
	UndoStatus     string    `json:"undo_status"`
	CreatedAt      time.Time `json:"created_at"`
}

type PaginatedResponse[T any] struct {
	Data       []T `json:"data"`
	Page       int `json:"page"`
	PageSize   int `json:"page_size"`
	TotalCount int `json:"total_count"`
	TotalPages int `json:"total_pages"`
}
