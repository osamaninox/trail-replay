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

func TestRevertJobTransaction_ToResponse(t *testing.T) {
	now := time.Now()
	txn := &domain.RevertJobTransaction{
		JobID:         uuid.New(),
		Seq:           3,
		TransactionID: 42,
		CommitLSN:     "0/1A2B3C0",
		ChangeCount:   7,
		Status:        domain.RevertTransactionStatusApplied,
		ErrorMessage:  "boom",
		AppliedAt:     &now,
	}

	resp := txn.ToResponse()

	if resp.Seq != 3 {
		t.Errorf("expected Seq=3, got %d", resp.Seq)
	}
	if resp.TransactionID != 42 {
		t.Errorf("expected TransactionID=42, got %d", resp.TransactionID)
	}
	if resp.CommitLSN != "0/1A2B3C0" {
		t.Errorf("expected CommitLSN=0/1A2B3C0, got %s", resp.CommitLSN)
	}
	if resp.ChangeCount != 7 {
		t.Errorf("expected ChangeCount=7, got %d", resp.ChangeCount)
	}
	if resp.Status != "applied" {
		t.Errorf("expected status applied, got %s", resp.Status)
	}
	if resp.ErrorMessage != "boom" {
		t.Errorf("expected ErrorMessage=boom, got %s", resp.ErrorMessage)
	}
	if resp.AppliedAt == nil || !resp.AppliedAt.Equal(now) {
		t.Errorf("expected AppliedAt=%v, got %v", now, resp.AppliedAt)
	}
}
