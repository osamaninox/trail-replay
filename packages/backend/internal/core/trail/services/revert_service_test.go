package services_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"trail-replay/internal/core/trail/domain"
	"trail-replay/internal/core/trail/ports/inbound"
	"trail-replay/internal/core/trail/services"
)

type mockRevertRepository struct {
	jobs   map[uuid.UUID]*domain.RevertJob
	err    error
	change int
}

func newMockRevertRepository() *mockRevertRepository {
	return &mockRevertRepository{
		jobs: make(map[uuid.UUID]*domain.RevertJob),
	}
}

func (m *mockRevertRepository) CreateJob(ctx context.Context, job *domain.RevertJob) error {
	if m.err != nil {
		return m.err
	}
	m.jobs[job.ID] = job
	return nil
}

func (m *mockRevertRepository) GetJob(ctx context.Context, id uuid.UUID) (*domain.RevertJob, error) {
	if m.err != nil {
		return nil, m.err
	}
	job, ok := m.jobs[id]
	if !ok {
		return nil, nil
	}
	return job, nil
}

func (m *mockRevertRepository) ListJobs(ctx context.Context, offset, limit int) ([]domain.RevertJob, error) {
	if m.err != nil {
		return nil, m.err
	}
	var result []domain.RevertJob
	for _, j := range m.jobs {
		result = append(result, *j)
	}
	if offset >= len(result) {
		return nil, nil
	}
	end := offset + limit
	if end > len(result) {
		end = len(result)
	}
	return result[offset:end], nil
}

func (m *mockRevertRepository) CountJobs(ctx context.Context) (int, error) {
	if m.err != nil {
		return 0, m.err
	}
	return len(m.jobs), nil
}

func (m *mockRevertRepository) UpdateJob(ctx context.Context, job *domain.RevertJob) error {
	if m.err != nil {
		return m.err
	}
	m.jobs[job.ID] = job
	return nil
}

func (m *mockRevertRepository) GetNextPendingJob(ctx context.Context) (*domain.RevertJob, error) {
	if m.err != nil {
		return nil, m.err
	}
	for _, j := range m.jobs {
		if j.Status == domain.RevertJobStatusPending {
			return j, nil
		}
	}
	return nil, nil
}

func (m *mockRevertRepository) GetActiveJob(ctx context.Context) (*domain.RevertJob, error) {
	if m.err != nil {
		return nil, m.err
	}
	for _, j := range m.jobs {
		if j.Status == domain.RevertJobStatusInProgress {
			return j, nil
		}
	}
	return nil, nil
}

func (m *mockRevertRepository) CountUnappliedChanges(ctx context.Context, from, to time.Time) (int, error) {
	if m.err != nil {
		return 0, m.err
	}
	return m.change, nil
}

func (m *mockRevertRepository) FetchUnappliedChanges(ctx context.Context, from, to time.Time, offset, limit int) ([]domain.WalChangeResult, error) {
	if m.err != nil {
		return nil, m.err
	}
	return nil, nil
}

func (m *mockRevertRepository) MarkChangeApplied(ctx context.Context, jobID uuid.UUID, change domain.WalChangeResult) error {
	return m.err
}

func (m *mockRevertRepository) MarkChangeFailed(ctx context.Context, jobID uuid.UUID, change domain.WalChangeResult, errMsg string) error {
	return m.err
}

func (m *mockRevertRepository) TryAcquireAdvisoryLock(ctx context.Context) (bool, error) {
	if m.err != nil {
		return false, m.err
	}
	return true, nil
}

func (m *mockRevertRepository) ReleaseAdvisoryLock(ctx context.Context) error {
	return m.err
}

func newRevertService(repo *mockRevertRepository) inbound.RevertService {
	return services.NewRevertService(repo, nil)
}

func TestCreateRevertJob_ValidTimeRange(t *testing.T) {
	ctx := context.Background()
	repo := newMockRevertRepository()
	repo.change = 42
	svc := newRevertService(repo)

	from := time.Now().Add(-1 * time.Hour)
	to := time.Now()
	job, err := svc.CreateRevertJob(ctx, from, to)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if job.Status != domain.RevertJobStatusPending {
		t.Errorf("expected pending, got %s", job.Status)
	}
	if job.TotalChanges != 42 {
		t.Errorf("expected TotalChanges=42, got %d", job.TotalChanges)
	}
	if !job.InputFrom.Equal(from) {
		t.Errorf("from mismatch")
	}
	if !job.InputTo.Equal(to) {
		t.Errorf("to mismatch")
	}
}

func TestCreateRevertJob_InvalidTimeRange(t *testing.T) {
	ctx := context.Background()
	repo := newMockRevertRepository()
	svc := newRevertService(repo)

	from := time.Now()
	to := time.Now().Add(-1 * time.Hour)
	_, err := svc.CreateRevertJob(ctx, from, to)
	if err == nil {
		t.Fatal("expected error for from > to")
	}
}

func TestCreateRevertJob_ZeroTimeRange(t *testing.T) {
	ctx := context.Background()
	repo := newMockRevertRepository()
	svc := newRevertService(repo)

	tm := time.Now()
	_, err := svc.CreateRevertJob(ctx, tm, tm)
	if err == nil {
		t.Fatal("expected error for from == to")
	}
}

func TestCreateRevertJob_RepoError(t *testing.T) {
	ctx := context.Background()
	repo := newMockRevertRepository()
	repo.err = errors.New("db error")
	svc := newRevertService(repo)

	from := time.Now().Add(-1 * time.Hour)
	to := time.Now()
	_, err := svc.CreateRevertJob(ctx, from, to)
	if err == nil {
		t.Fatal("expected error from repo")
	}
}

func TestGetRevertJob_Found(t *testing.T) {
	ctx := context.Background()
	repo := newMockRevertRepository()
	svc := newRevertService(repo)

	from := time.Now().Add(-1 * time.Hour)
	to := time.Now()
	created, _ := svc.CreateRevertJob(ctx, from, to)

	got, err := svc.GetRevertJob(ctx, created.ID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.ID != created.ID {
		t.Errorf("id mismatch")
	}
}

func TestGetRevertJob_NotFound(t *testing.T) {
	ctx := context.Background()
	repo := newMockRevertRepository()
	svc := newRevertService(repo)

	_, err := svc.GetRevertJob(ctx, uuid.New())
	if err == nil {
		t.Fatal("expected error for not found")
	}
}

func TestListRevertJobs_Empty(t *testing.T) {
	ctx := context.Background()
	repo := newMockRevertRepository()
	svc := newRevertService(repo)

	result, err := svc.ListRevertJobs(ctx, 1, 20)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.Data) != 0 {
		t.Errorf("expected 0 jobs, got %d", len(result.Data))
	}
	if result.TotalCount != 0 {
		t.Errorf("expected TotalCount=0, got %d", result.TotalCount)
	}
	if result.TotalPages != 0 {
		t.Errorf("expected TotalPages=0, got %d", result.TotalPages)
	}
}

func TestListRevertJobs_Multiple(t *testing.T) {
	ctx := context.Background()
	repo := newMockRevertRepository()
	svc := newRevertService(repo)

	from := time.Now().Add(-2 * time.Hour)
	to := time.Now()
	svc.CreateRevertJob(ctx, from, to)
	svc.CreateRevertJob(ctx, from, to)

	result, err := svc.ListRevertJobs(ctx, 1, 20)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.Data) != 2 {
		t.Errorf("expected 2 jobs, got %d", len(result.Data))
	}
	if result.TotalCount != 2 {
		t.Errorf("expected TotalCount=2, got %d", result.TotalCount)
	}
}

func TestCancelRevertJob_ValidPending(t *testing.T) {
	ctx := context.Background()
	repo := newMockRevertRepository()
	svc := newRevertService(repo)

	from := time.Now().Add(-1 * time.Hour)
	to := time.Now()
	job, _ := svc.CreateRevertJob(ctx, from, to)

	cancelled, err := svc.CancelRevertJob(ctx, job.ID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cancelled.Status != domain.RevertJobStatusCancelled {
		t.Errorf("expected cancelled, got %s", cancelled.Status)
	}
}

func TestCancelRevertJob_ValidInProgress(t *testing.T) {
	ctx := context.Background()
	repo := newMockRevertRepository()
	svc := newRevertService(repo)

	from := time.Now().Add(-1 * time.Hour)
	to := time.Now()
	job, _ := svc.CreateRevertJob(ctx, from, to)
	job.Status = domain.RevertJobStatusInProgress
	repo.jobs[job.ID] = job

	cancelled, err := svc.CancelRevertJob(ctx, job.ID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cancelled.Status != domain.RevertJobStatusCancelling {
		t.Errorf("expected cancelling, got %s", cancelled.Status)
	}
}

func TestCancelRevertJob_AlreadyTerminal(t *testing.T) {
	ctx := context.Background()
	repo := newMockRevertRepository()
	svc := newRevertService(repo)

	from := time.Now().Add(-1 * time.Hour)
	to := time.Now()
	job, _ := svc.CreateRevertJob(ctx, from, to)
	job.Status = domain.RevertJobStatusCompleted
	repo.jobs[job.ID] = job

	_, err := svc.CancelRevertJob(ctx, job.ID)
	if err == nil {
		t.Fatal("expected error for cancelling completed job")
	}
}

func TestCancelRevertJob_NotFound(t *testing.T) {
	ctx := context.Background()
	repo := newMockRevertRepository()
	svc := newRevertService(repo)

	_, err := svc.CancelRevertJob(ctx, uuid.New())
	if err == nil {
		t.Fatal("expected error for not found")
	}
}
