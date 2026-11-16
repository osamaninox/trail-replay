package domain

import (
	"time"

	"github.com/google/uuid"
)

type RevertJobStatus string

const (
	RevertJobStatusPending    RevertJobStatus = "pending"
	RevertJobStatusInProgress RevertJobStatus = "in_progress"
	RevertJobStatusCompleted  RevertJobStatus = "completed"
	RevertJobStatusFailed     RevertJobStatus = "failed"
	RevertJobStatusCancelling RevertJobStatus = "cancelling"
	RevertJobStatusCancelled  RevertJobStatus = "cancelled"
)

type RevertJob struct {
	ID             uuid.UUID
	Status         RevertJobStatus
	InputFrom      time.Time
	InputTo        time.Time
	TotalChanges   int
	CompletedCount int
	FailedCount    int
	LastError      string
	CreatedAt      time.Time
	UpdatedAt      time.Time
	CompletedAt    *time.Time
}

func NewRevertJob(from, to time.Time) *RevertJob {
	now := time.Now()
	return &RevertJob{
		ID:        uuid.New(),
		Status:    RevertJobStatusPending,
		InputFrom: from,
		InputTo:   to,
		CreatedAt: now,
		UpdatedAt: now,
	}
}

func (j *RevertJob) CanCancel() bool {
	return j.Status == RevertJobStatusPending || j.Status == RevertJobStatusInProgress
}

func (j *RevertJob) IsTerminal() bool {
	return j.Status == RevertJobStatusCompleted ||
		j.Status == RevertJobStatusFailed ||
		j.Status == RevertJobStatusCancelled
}

type RevertTransactionStatus string

const (
	RevertTransactionStatusPending    RevertTransactionStatus = "pending"
	RevertTransactionStatusInProgress RevertTransactionStatus = "in_progress"
	RevertTransactionStatusApplied    RevertTransactionStatus = "applied"
	RevertTransactionStatusFailed     RevertTransactionStatus = "failed"
	RevertTransactionStatusSkipped    RevertTransactionStatus = "skipped"
)

// RevertJobTransaction is one unit of work in a revert job's plan snapshot.
// seq is the transaction's position in the revert order (commit_lsn DESC) and
// maps 1:1 to the DBOS workflow step index that reverts it.
type RevertJobTransaction struct {
	JobID         uuid.UUID
	Seq           int
	TransactionID uint64
	CommitLSN     string
	ChangeCount   int
	Status        RevertTransactionStatus
	ErrorMessage  string
	AppliedAt     *time.Time
}

type RevertJobTransactionResponse struct {
	Seq           int        `json:"seq"`
	TransactionID uint64     `json:"transaction_id"`
	CommitLSN     string     `json:"commit_lsn"`
	ChangeCount   int        `json:"change_count"`
	Status        string     `json:"status"`
	ErrorMessage  string     `json:"error_message,omitempty"`
	AppliedAt     *time.Time `json:"applied_at,omitempty"`
}

func (t *RevertJobTransaction) ToResponse() RevertJobTransactionResponse {
	return RevertJobTransactionResponse{
		Seq:           t.Seq,
		TransactionID: t.TransactionID,
		CommitLSN:     t.CommitLSN,
		ChangeCount:   t.ChangeCount,
		Status:        string(t.Status),
		ErrorMessage:  t.ErrorMessage,
		AppliedAt:     t.AppliedAt,
	}
}

type CreateRevertJobRequest struct {
	From time.Time `json:"from"`
	To   time.Time `json:"to"`
}

type RevertJobResponse struct {
	ID             uuid.UUID  `json:"id"`
	Status         string     `json:"status"`
	InputFrom      time.Time  `json:"input_from"`
	InputTo        time.Time  `json:"input_to"`
	TotalChanges   int        `json:"total_changes"`
	CompletedCount int        `json:"completed_count"`
	FailedCount    int        `json:"failed_count"`
	LastError      string     `json:"last_error,omitempty"`
	CreatedAt      time.Time  `json:"created_at"`
	UpdatedAt      time.Time  `json:"updated_at"`
	CompletedAt    *time.Time `json:"completed_at,omitempty"`
}

func (j *RevertJob) ToResponse() RevertJobResponse {
	return RevertJobResponse{
		ID:             j.ID,
		Status:         string(j.Status),
		InputFrom:      j.InputFrom,
		InputTo:        j.InputTo,
		TotalChanges:   j.TotalChanges,
		CompletedCount: j.CompletedCount,
		FailedCount:    j.FailedCount,
		LastError:      j.LastError,
		CreatedAt:      j.CreatedAt,
		UpdatedAt:      j.UpdatedAt,
		CompletedAt:    j.CompletedAt,
	}
}
