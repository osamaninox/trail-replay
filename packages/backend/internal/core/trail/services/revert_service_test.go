package services_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"trail-replay/internal/core/trail/domain"
	"trail-replay/internal/core/trail/ports/inbound"
	"trail-replay/internal/core/trail/ports/outbound"
	"trail-replay/internal/core/trail/services"
)

type mockRevertRepository struct {
	jobs    map[uuid.UUID]*domain.RevertJob
	txns    map[uuid.UUID][]domain.RevertJobTransaction
	err     error
	count   int
	changes []domain.RevertChange
}

func newMockRevertRepository() *mockRevertRepository {
	return &mockRevertRepository{
		jobs: make(map[uuid.UUID]*domain.RevertJob),
		txns: make(map[uuid.UUID][]domain.RevertJobTransaction),
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

func (m *mockRevertRepository) CountRevertableTransactions(ctx context.Context, from, to time.Time) (int, error) {
	if m.err != nil {
		return 0, m.err
	}
	return m.count, nil
}

func (m *mockRevertRepository) ListRevertableChanges(ctx context.Context, from, to time.Time) ([]domain.RevertChange, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.changes, nil
}

func (m *mockRevertRepository) SnapshotPlan(ctx context.Context, jobID uuid.UUID, from, to time.Time) (int, int, error) {
	return 0, 0, m.err
}

func (m *mockRevertRepository) GetJobTransaction(ctx context.Context, jobID uuid.UUID, seq int) (*domain.RevertJobTransaction, error) {
	return nil, m.err
}

func (m *mockRevertRepository) ListJobTransactions(ctx context.Context, jobID uuid.UUID, offset, limit int) ([]domain.RevertJobTransaction, error) {
	if m.err != nil {
		return nil, m.err
	}
	txns := m.txns[jobID]
	if offset >= len(txns) {
		return nil, nil
	}
	end := offset + limit
	if end > len(txns) {
		end = len(txns)
	}
	return txns[offset:end], nil
}

func (m *mockRevertRepository) CountJobTransactions(ctx context.Context, jobID uuid.UUID) (int, error) {
	if m.err != nil {
		return 0, m.err
	}
	return len(m.txns[jobID]), nil
}

func (m *mockRevertRepository) GetTransactionChanges(ctx context.Context, transactionID uint64) ([]domain.WalChangeResult, error) {
	return nil, m.err
}

func (m *mockRevertRepository) MarkTransactionApplied(ctx context.Context, tx outbound.SQLTx, jobID uuid.UUID, seq int) error {
	return m.err
}

func (m *mockRevertRepository) MarkTransactionFailed(ctx context.Context, tx outbound.SQLTx, jobID uuid.UUID, seq int, errMsg string) error {
	return m.err
}

func (m *mockRevertRepository) ListPendingJobIDs(ctx context.Context) ([]uuid.UUID, error) {
	return nil, m.err
}

type fakeSubmitter struct {
	submitted []uuid.UUID
	cancelled []uuid.UUID
	err       error
}

func (f *fakeSubmitter) Submit(ctx context.Context, jobID uuid.UUID) error {
	if f.err != nil {
		return f.err
	}
	f.submitted = append(f.submitted, jobID)
	return nil
}

func (f *fakeSubmitter) Cancel(ctx context.Context, jobID uuid.UUID) error {
	if f.err != nil {
		return f.err
	}
	f.cancelled = append(f.cancelled, jobID)
	return nil
}

func newRevertService(repo *mockRevertRepository, submitter *fakeSubmitter) inbound.RevertService {
	return services.NewRevertService(repo, submitter, nil)
}

func TestCreateRevertJob_ValidTimeRange(t *testing.T) {
	ctx := context.Background()
	repo := newMockRevertRepository()
	repo.count = 42
	submitter := &fakeSubmitter{}
	svc := newRevertService(repo, submitter)

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
	if len(submitter.submitted) != 1 || submitter.submitted[0] != job.ID {
		t.Errorf("expected Submit(%s), got %v", job.ID, submitter.submitted)
	}
}

func TestCreateRevertJob_InvalidTimeRange(t *testing.T) {
	ctx := context.Background()
	repo := newMockRevertRepository()
	submitter := &fakeSubmitter{}
	svc := newRevertService(repo, submitter)

	from := time.Now()
	to := time.Now().Add(-1 * time.Hour)
	_, err := svc.CreateRevertJob(ctx, from, to)
	if err == nil {
		t.Fatal("expected error for from > to")
	}
	if len(submitter.submitted) != 0 {
		t.Errorf("expected no submit on invalid range, got %v", submitter.submitted)
	}
}

func TestCreateRevertJob_ZeroTimeRange(t *testing.T) {
	ctx := context.Background()
	repo := newMockRevertRepository()
	submitter := &fakeSubmitter{}
	svc := newRevertService(repo, submitter)

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
	submitter := &fakeSubmitter{}
	svc := newRevertService(repo, submitter)

	from := time.Now().Add(-1 * time.Hour)
	to := time.Now()
	_, err := svc.CreateRevertJob(ctx, from, to)
	if err == nil {
		t.Fatal("expected error from repo")
	}
}

func TestCreateRevertJob_SubmitError(t *testing.T) {
	ctx := context.Background()
	repo := newMockRevertRepository()
	submitter := &fakeSubmitter{err: errors.New("engine unavailable")}
	svc := newRevertService(repo, submitter)

	from := time.Now().Add(-1 * time.Hour)
	to := time.Now()
	_, err := svc.CreateRevertJob(ctx, from, to)
	if err == nil {
		t.Fatal("expected error from submitter")
	}
	// Job row persists as pending for startup reconciliation (plan §4).
	if len(repo.jobs) != 1 {
		t.Errorf("expected job row to persist for reconciliation, got %d jobs", len(repo.jobs))
	}
}

func TestGetRevertJob_Found(t *testing.T) {
	ctx := context.Background()
	repo := newMockRevertRepository()
	submitter := &fakeSubmitter{}
	svc := newRevertService(repo, submitter)

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
	submitter := &fakeSubmitter{}
	svc := newRevertService(repo, submitter)

	_, err := svc.GetRevertJob(ctx, uuid.New())
	if err == nil {
		t.Fatal("expected error for not found")
	}
}

func TestListRevertJobs_Empty(t *testing.T) {
	ctx := context.Background()
	repo := newMockRevertRepository()
	submitter := &fakeSubmitter{}
	svc := newRevertService(repo, submitter)

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
	submitter := &fakeSubmitter{}
	svc := newRevertService(repo, submitter)

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

func TestListJobTransactions_JobNotFound(t *testing.T) {
	ctx := context.Background()
	repo := newMockRevertRepository()
	submitter := &fakeSubmitter{}
	svc := newRevertService(repo, submitter)

	_, err := svc.ListJobTransactions(ctx, uuid.New(), 1, 20)
	if err == nil {
		t.Fatal("expected error for unknown job")
	}
}

func TestListJobTransactions_Pagination(t *testing.T) {
	ctx := context.Background()
	repo := newMockRevertRepository()
	submitter := &fakeSubmitter{}
	svc := newRevertService(repo, submitter)

	from := time.Now().Add(-1 * time.Hour)
	to := time.Now()
	job, _ := svc.CreateRevertJob(ctx, from, to)

	for seq := 1; seq <= 5; seq++ {
		repo.txns[job.ID] = append(repo.txns[job.ID], domain.RevertJobTransaction{
			JobID: job.ID,
			Seq:   seq,
		})
	}

	page1, err := svc.ListJobTransactions(ctx, job.ID, 1, 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(page1.Data) != 2 || page1.Data[0].Seq != 1 || page1.Data[1].Seq != 2 {
		t.Errorf("page 1 data mismatch: %+v", page1.Data)
	}
	if page1.TotalCount != 5 || page1.TotalPages != 3 {
		t.Errorf("expected TotalCount=5 TotalPages=3, got %d/%d", page1.TotalCount, page1.TotalPages)
	}

	page3, err := svc.ListJobTransactions(ctx, job.ID, 3, 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(page3.Data) != 1 || page3.Data[0].Seq != 5 {
		t.Errorf("page 3 data mismatch: %+v", page3.Data)
	}
}

func TestCancelRevertJob_ValidPending(t *testing.T) {
	ctx := context.Background()
	repo := newMockRevertRepository()
	submitter := &fakeSubmitter{}
	svc := newRevertService(repo, submitter)

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
	if len(submitter.cancelled) != 1 || submitter.cancelled[0] != job.ID {
		t.Errorf("expected Cancel(%s), got %v", job.ID, submitter.cancelled)
	}
}

func TestCancelRevertJob_ValidInProgress(t *testing.T) {
	ctx := context.Background()
	repo := newMockRevertRepository()
	submitter := &fakeSubmitter{}
	svc := newRevertService(repo, submitter)

	from := time.Now().Add(-1 * time.Hour)
	to := time.Now()
	job, _ := svc.CreateRevertJob(ctx, from, to)
	job.Status = domain.RevertJobStatusInProgress
	repo.jobs[job.ID] = job

	cancelled, err := svc.CancelRevertJob(ctx, job.ID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cancelled.Status != domain.RevertJobStatusCancelled {
		t.Errorf("expected cancelled, got %s", cancelled.Status)
	}
	if len(submitter.cancelled) != 1 || submitter.cancelled[0] != job.ID {
		t.Errorf("expected Cancel(%s), got %v", job.ID, submitter.cancelled)
	}
}

func TestCancelRevertJob_SubmitCancelError(t *testing.T) {
	ctx := context.Background()
	repo := newMockRevertRepository()
	svc := newRevertService(repo, &fakeSubmitter{})

	from := time.Now().Add(-1 * time.Hour)
	to := time.Now()
	job, _ := svc.CreateRevertJob(ctx, from, to)

	failing := &fakeSubmitter{err: errors.New("engine unavailable")}
	svc = services.NewRevertService(repo, failing, nil)

	_, err := svc.CancelRevertJob(ctx, job.ID)
	if err == nil {
		t.Fatal("expected error from submitter cancel")
	}
}

func TestCancelRevertJob_AlreadyTerminal(t *testing.T) {
	ctx := context.Background()
	repo := newMockRevertRepository()
	submitter := &fakeSubmitter{}
	svc := newRevertService(repo, submitter)

	from := time.Now().Add(-1 * time.Hour)
	to := time.Now()
	job, _ := svc.CreateRevertJob(ctx, from, to)
	job.Status = domain.RevertJobStatusCompleted
	repo.jobs[job.ID] = job

	_, err := svc.CancelRevertJob(ctx, job.ID)
	if err == nil {
		t.Fatal("expected error for cancelling completed job")
	}
	if len(submitter.cancelled) != 0 {
		t.Errorf("expected no cancel call for terminal job, got %v", submitter.cancelled)
	}
}

func TestCancelRevertJob_NotFound(t *testing.T) {
	ctx := context.Background()
	repo := newMockRevertRepository()
	submitter := &fakeSubmitter{}
	svc := newRevertService(repo, submitter)

	_, err := svc.CancelRevertJob(ctx, uuid.New())
	if err == nil {
		t.Fatal("expected error for not found")
	}
}
