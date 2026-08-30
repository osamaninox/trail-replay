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

type RevertChangeStatus string

const (
	RevertChangeStatusPending RevertChangeStatus = "pending"
	RevertChangeStatusApplied RevertChangeStatus = "applied"
	RevertChangeStatusFailed  RevertChangeStatus = "failed"
	RevertChangeStatusSkipped RevertChangeStatus = "skipped"
)

type RevertJobChange struct {
	JobID        uuid.UUID
	ChangeID     uint64
	Status       RevertChangeStatus
	ErrorMessage string
	AppliedAt    *time.Time
}

func NewRevertJobChange(jobID uuid.UUID, changeID uint64) *RevertJobChange {
	return &RevertJobChange{
		JobID:    jobID,
		ChangeID: changeID,
		Status:   RevertChangeStatusPending,
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
