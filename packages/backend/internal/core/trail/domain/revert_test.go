package domain_test

import (
	"testing"
	"time"

	"github.com/google/uuid"

	"trail-replay/internal/core/trail/domain"
)

func TestNewRevertJob_Defaults(t *testing.T) {
	from := time.Now().Add(-1 * time.Hour)
	to := time.Now()
	job := domain.NewRevertJob(from, to)

	if job.ID == uuid.Nil {
		t.Error("expected non-nil UUID")
	}
	if job.Status != domain.RevertJobStatusPending {
		t.Errorf("expected status pending, got %s", job.Status)
	}
	if !job.InputFrom.Equal(from) {
		t.Errorf("expected from %v, got %v", from, job.InputFrom)
	}
	if !job.InputTo.Equal(to) {
		t.Errorf("expected to %v, got %v", to, job.InputTo)
	}
	if job.TotalChanges != 0 {
		t.Errorf("expected TotalChanges=0, got %d", job.TotalChanges)
	}
	if job.CompletedCount != 0 {
		t.Errorf("expected CompletedCount=0, got %d", job.CompletedCount)
	}
	if job.FailedCount != 0 {
		t.Errorf("expected FailedCount=0, got %d", job.FailedCount)
	}
	if job.LastError != "" {
		t.Errorf("expected empty LastError, got %s", job.LastError)
	}
	if job.CompletedAt != nil {
		t.Error("expected nil CompletedAt")
	}
	if job.CreatedAt.IsZero() {
		t.Error("expected non-zero CreatedAt")
	}
	if job.UpdatedAt.IsZero() {
		t.Error("expected non-zero UpdatedAt")
	}
}

func TestRevertJob_CanCancel(t *testing.T) {
	job := domain.NewRevertJob(time.Now().Add(-1*time.Hour), time.Now())

	if !job.CanCancel() {
		t.Error("pending job should be cancellable")
	}

	job.Status = domain.RevertJobStatusInProgress
	if !job.CanCancel() {
		t.Error("in_progress job should be cancellable")
	}

	job.Status = domain.RevertJobStatusCompleted
	if job.CanCancel() {
		t.Error("completed job should not be cancellable")
	}

	job.Status = domain.RevertJobStatusFailed
	if job.CanCancel() {
		t.Error("failed job should not be cancellable")
	}

	job.Status = domain.RevertJobStatusCancelled
	if job.CanCancel() {
		t.Error("cancelled job should not be cancellable")
	}

	job.Status = domain.RevertJobStatusCancelling
	if job.CanCancel() {
		t.Error("cancelling job should not be cancellable")
	}
}

func TestRevertJob_IsTerminal(t *testing.T) {
	job := domain.NewRevertJob(time.Now().Add(-1*time.Hour), time.Now())

	terminal := []domain.RevertJobStatus{
		domain.RevertJobStatusCompleted,
		domain.RevertJobStatusFailed,
		domain.RevertJobStatusCancelled,
	}

	nonTerminal := []domain.RevertJobStatus{
		domain.RevertJobStatusPending,
		domain.RevertJobStatusInProgress,
		domain.RevertJobStatusCancelling,
	}

	for _, s := range terminal {
		job.Status = s
		if !job.IsTerminal() {
			t.Errorf("status %s should be terminal", s)
		}
	}

	for _, s := range nonTerminal {
		job.Status = s
		if job.IsTerminal() {
			t.Errorf("status %s should not be terminal", s)
		}
	}
}

func TestRevertJobChange_Defaults(t *testing.T) {
	jobID := uuid.New()
	changeID := uint64(42)
	change := domain.NewRevertJobChange(jobID, changeID)

	if change.JobID != jobID {
		t.Errorf("expected JobID=%s, got %s", jobID, change.JobID)
	}
	if change.ChangeID != changeID {
		t.Errorf("expected ChangeID=%d, got %d", changeID, change.ChangeID)
	}
	if change.Status != domain.RevertChangeStatusPending {
		t.Errorf("expected status pending, got %s", change.Status)
	}
	if change.ErrorMessage != "" {
		t.Errorf("expected empty ErrorMessage, got %s", change.ErrorMessage)
	}
	if change.AppliedAt != nil {
		t.Error("expected nil AppliedAt")
	}
}
