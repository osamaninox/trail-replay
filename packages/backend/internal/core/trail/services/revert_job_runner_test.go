package services_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"trail-replay/internal/core/trail/domain"
	"trail-replay/internal/core/trail/services"
)

type runnerMockRepo struct {
	mu       sync.Mutex
	jobs     map[uuid.UUID]*domain.RevertJob
	changes  []domain.WalChangeResult
	err      error
	lockHeld bool

	cancellingJobID uuid.UUID

	appliedChanges []domain.WalChangeResult
	failedChanges  []domain.WalChangeResult
	failedMsgs     []string
	updatedJobs    []*domain.RevertJob
	fetchOffsets   []int
	fetchLimits    []int
}

func newRunnerMockRepo() *runnerMockRepo {
	return &runnerMockRepo{
		jobs:            make(map[uuid.UUID]*domain.RevertJob),
		cancellingJobID: uuid.Nil,
	}
}

func (m *runnerMockRepo) CreateJob(ctx context.Context, job *domain.RevertJob) error          { return nil }
func (m *runnerMockRepo) ListJobs(ctx context.Context, offset, limit int) ([]domain.RevertJob, error) { return nil, nil }
func (m *runnerMockRepo) CountJobs(ctx context.Context) (int, error)                        { return 0, nil }
func (m *runnerMockRepo) CountUnappliedChanges(ctx context.Context, from, to time.Time) (int, error) {
	return len(m.changes), nil
}

func (m *runnerMockRepo) GetJob(ctx context.Context, id uuid.UUID) (*domain.RevertJob, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	job, ok := m.jobs[id]
	if !ok {
		return nil, errors.New("not found")
	}
	result := *job
	if m.cancellingJobID == id {
		result.Status = domain.RevertJobStatusCancelling
	}
	return &result, nil
}

func (m *runnerMockRepo) UpdateJob(ctx context.Context, job *domain.RevertJob) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.updatedJobs = append(m.updatedJobs, job)
	copy := *job
	m.jobs[job.ID] = &copy
	return nil
}

func (m *runnerMockRepo) GetNextPendingJob(ctx context.Context) (*domain.RevertJob, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, j := range m.jobs {
		if j.Status == domain.RevertJobStatusPending {
			copy := *j
			return &copy, nil
		}
	}
	return nil, nil
}

func (m *runnerMockRepo) GetActiveJob(ctx context.Context) (*domain.RevertJob, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, j := range m.jobs {
		if j.Status == domain.RevertJobStatusInProgress {
			copy := *j
			return &copy, nil
		}
	}
	return nil, nil
}

func (m *runnerMockRepo) FetchUnappliedChanges(ctx context.Context, from, to time.Time, offset, limit int) ([]domain.WalChangeResult, error) {
	if m.err != nil {
		return nil, m.err
	}
	m.mu.Lock()
	m.fetchOffsets = append(m.fetchOffsets, offset)
	m.fetchLimits = append(m.fetchLimits, limit)
	m.mu.Unlock()

	if offset >= len(m.changes) {
		return nil, nil
	}
	end := offset + limit
	if end > len(m.changes) {
		end = len(m.changes)
	}
	return m.changes[offset:end], nil
}

func (m *runnerMockRepo) MarkChangeApplied(ctx context.Context, jobID uuid.UUID, change domain.WalChangeResult) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.appliedChanges = append(m.appliedChanges, change)
	return nil
}

func (m *runnerMockRepo) MarkChangeFailed(ctx context.Context, jobID uuid.UUID, change domain.WalChangeResult, errMsg string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failedChanges = append(m.failedChanges, change)
	m.failedMsgs = append(m.failedMsgs, errMsg)
	return nil
}

func (m *runnerMockRepo) TryAcquireAdvisoryLock(ctx context.Context) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return false, m.err
	}
	if m.lockHeld {
		return false, nil
	}
	m.lockHeld = true
	return true, nil
}

func (m *runnerMockRepo) ReleaseAdvisoryLock(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lockHeld = false
	return nil
}

type mockExecutor struct {
	mu       sync.Mutex
	execSQL  []string
	err      error
	delay    time.Duration
}

func (m *mockExecutor) Exec(ctx context.Context, sql string) error {
	if m.delay > 0 {
		time.Sleep(m.delay)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.execSQL = append(m.execSQL, sql)
	return m.err
}

func TestRunner_PicksUpPendingJob(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	repo := newRunnerMockRepo()
	executor := &mockExecutor{}

	from := time.Now().Add(-1 * time.Hour)
	to := time.Now()
	job := domain.NewRevertJob(from, to)
	repo.jobs[job.ID] = job

	jobCreated := make(chan struct{}, 1)
	jobCreated <- struct{}{}

	go func() {
		services.StartRevertJobRunner(ctx, repo, executor, jobCreated)
	}()

	time.Sleep(100 * time.Millisecond)
	cancel()

	repo.mu.Lock()
	defer repo.mu.Unlock()

	if len(repo.updatedJobs) == 0 {
		t.Fatal("expected job to be updated")
	}

	updated := false
	for _, u := range repo.updatedJobs {
		if u.ID == job.ID && u.Status != domain.RevertJobStatusPending {
			updated = true
			break
		}
	}
	if !updated {
		t.Error("expected job to have been processed to non-pending status")
	}
}

func TestRunner_ResumesActiveJob(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	repo := newRunnerMockRepo()
	executor := &mockExecutor{}

	from := time.Now().Add(-1 * time.Hour)
	to := time.Now()
	job := domain.NewRevertJob(from, to)
	job.Status = domain.RevertJobStatusInProgress
	job.TotalChanges = 1
	repo.jobs[job.ID] = job
	repo.changes = []domain.WalChangeResult{
		{ID: 1, ReverseDMLSQL: "DELETE FROM x WHERE id=1"},
	}

	jobCreated := make(chan struct{}, 1)

	go func() {
		services.StartRevertJobRunner(ctx, repo, executor, jobCreated)
	}()

	time.Sleep(100 * time.Millisecond)
	cancel()

	repo.mu.Lock()
	defer repo.mu.Unlock()

	if len(repo.appliedChanges) != 1 {
		t.Errorf("expected 1 applied change, got %d", len(repo.appliedChanges))
	}
}

func TestRunner_SkipsWhenLockHeld(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	repo := newRunnerMockRepo()
	repo.lockHeld = true

	from := time.Now().Add(-1 * time.Hour)
	to := time.Now()
	job := domain.NewRevertJob(from, to)
	repo.jobs[job.ID] = job

	executor := &mockExecutor{}
	jobCreated := make(chan struct{}, 1)
	jobCreated <- struct{}{}

	go func() {
		services.StartRevertJobRunner(ctx, repo, executor, jobCreated)
	}()

	time.Sleep(100 * time.Millisecond)
	cancel()

	repo.mu.Lock()
	defer repo.mu.Unlock()

	if len(repo.appliedChanges) != 0 {
		t.Errorf("expected 0 applied changes when lock held, got %d", len(repo.appliedChanges))
	}
}

func TestRunner_ProcessesChangesInBatches(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	repo := newRunnerMockRepo()
	executor := &mockExecutor{}

	changes := make([]domain.WalChangeResult, 250)
	for i := range changes {
		changes[i] = domain.WalChangeResult{
			ID:            uint64(i + 1),
			ReverseDMLSQL: "SELECT 1",
		}
	}
	repo.changes = changes

	from := time.Now().Add(-1 * time.Hour)
	to := time.Now()
	job := domain.NewRevertJob(from, to)
	job.TotalChanges = 250
	repo.jobs[job.ID] = job

	jobCreated := make(chan struct{}, 1)
	jobCreated <- struct{}{}

	go func() {
		services.StartRevertJobRunner(ctx, repo, executor, jobCreated)
	}()

	time.Sleep(500 * time.Millisecond)
	cancel()

	repo.mu.Lock()
	defer repo.mu.Unlock()

	if len(repo.appliedChanges) != 250 {
		t.Errorf("expected 250 applied changes, got %d", len(repo.appliedChanges))
	}

	if len(repo.fetchOffsets) == 0 {
		t.Fatal("expected fetch calls")
	}

	if len(repo.fetchOffsets) != 3 {
		t.Errorf("expected 3 batches (250/100=3), got offsets %v", repo.fetchOffsets)
	}

	if repo.fetchOffsets[0] != 0 || repo.fetchOffsets[1] != 100 || repo.fetchOffsets[2] != 200 {
		t.Errorf("expected offsets [0,100,200], got %v", repo.fetchOffsets)
	}
}

func TestRunner_HandlesExecutorError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	repo := newRunnerMockRepo()
	executor := &mockExecutor{err: errors.New("exec failed")}

	repo.changes = []domain.WalChangeResult{
		{ID: 1, ReverseDMLSQL: "DELETE FROM x WHERE id=1"},
		{ID: 2, ReverseDMLSQL: "DELETE FROM y WHERE id=2"},
	}

	from := time.Now().Add(-1 * time.Hour)
	to := time.Now()
	job := domain.NewRevertJob(from, to)
	job.TotalChanges = 2
	repo.jobs[job.ID] = job

	jobCreated := make(chan struct{}, 1)
	jobCreated <- struct{}{}

	go func() {
		services.StartRevertJobRunner(ctx, repo, executor, jobCreated)
	}()

	time.Sleep(200 * time.Millisecond)
	cancel()

	repo.mu.Lock()
	defer repo.mu.Unlock()

	if len(repo.failedChanges) != 2 {
		t.Errorf("expected 2 failed changes, got %d", len(repo.failedChanges))
	}

	hasFailed := false
	for _, u := range repo.updatedJobs {
		if u.Status == domain.RevertJobStatusFailed {
			hasFailed = true
			break
		}
	}
	if !hasFailed {
		t.Error("expected job to be failed")
	}
}

func TestRunner_FinalizesCompleted(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	repo := newRunnerMockRepo()
	executor := &mockExecutor{}

	repo.changes = []domain.WalChangeResult{
		{ID: 1, ReverseDMLSQL: "DELETE FROM x WHERE id=1"},
	}

	from := time.Now().Add(-1 * time.Hour)
	to := time.Now()
	job := domain.NewRevertJob(from, to)
	job.TotalChanges = 1
	repo.jobs[job.ID] = job

	jobCreated := make(chan struct{}, 1)
	jobCreated <- struct{}{}

	go func() {
		services.StartRevertJobRunner(ctx, repo, executor, jobCreated)
	}()

	time.Sleep(100 * time.Millisecond)
	cancel()

	repo.mu.Lock()
	defer repo.mu.Unlock()

	if repo.lockHeld {
		t.Error("expected lock to be released after completion")
	}

	jobAfter := repo.jobs[job.ID]
	if jobAfter.Status != domain.RevertJobStatusCompleted {
		t.Errorf("expected completed, got %s", jobAfter.Status)
	}
	if jobAfter.CompletedAt == nil {
		t.Error("expected CompletedAt to be set")
	}
}

func TestRunner_RespondsToCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	repo := newRunnerMockRepo()
	executor := &mockExecutor{}

	changes := make([]domain.WalChangeResult, 250)
	for i := range changes {
		changes[i] = domain.WalChangeResult{
			ID:            uint64(i + 1),
			ReverseDMLSQL: "SELECT 1",
		}
	}
	repo.changes = changes

	from := time.Now().Add(-1 * time.Hour)
	to := time.Now()
	job := domain.NewRevertJob(from, to)
	job.TotalChanges = 250
	repo.jobs[job.ID] = job

	repo.mu.Lock()
	repo.cancellingJobID = job.ID
	repo.mu.Unlock()

	jobCreated := make(chan struct{}, 1)
	jobCreated <- struct{}{}

	go func() {
		services.StartRevertJobRunner(ctx, repo, executor, jobCreated)
	}()

	time.Sleep(100 * time.Millisecond)
	cancel()

	repo.mu.Lock()
	defer repo.mu.Unlock()

	jobAfter := repo.jobs[job.ID]
	if jobAfter.Status != domain.RevertJobStatusCancelled {
		t.Errorf("expected cancelled, got %s", jobAfter.Status)
	}

	if len(repo.appliedChanges) > 0 {
		t.Errorf("expected 0 applied changes when cancelled from start, got %d", len(repo.appliedChanges))
	}
}

func TestRunner_NoJobReleasesLock(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	repo := newRunnerMockRepo()
	executor := &mockExecutor{}

	jobCreated := make(chan struct{}, 1)
	jobCreated <- struct{}{}

	go func() {
		services.StartRevertJobRunner(ctx, repo, executor, jobCreated)
	}()

	time.Sleep(100 * time.Millisecond)
	cancel()

	repo.mu.Lock()
	defer repo.mu.Unlock()

	if repo.lockHeld {
		t.Error("expected lock to be released when no jobs available")
	}
}
