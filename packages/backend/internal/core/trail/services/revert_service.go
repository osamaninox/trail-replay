package services

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"

	"trail-replay/internal/core/trail/domain"
	"trail-replay/internal/core/trail/ports/inbound"
	"trail-replay/internal/core/trail/ports/outbound"
)

type revertService struct {
	repo       outbound.RevertRepository
	jobCreated chan<- struct{}
}

func NewRevertService(repo outbound.RevertRepository, jobCreated chan<- struct{}) inbound.RevertService {
	return &revertService{repo: repo, jobCreated: jobCreated}
}

func (s *revertService) CreateRevertJob(ctx context.Context, from, to time.Time) (*domain.RevertJob, error) {
	if !from.Before(to) {
		return nil, errors.New("from must be before to")
	}

	total, err := s.repo.CountUnappliedChanges(ctx, from, to)
	if err != nil {
		return nil, err
	}

	job := domain.NewRevertJob(from, to)
	job.TotalChanges = total
	if err := s.repo.CreateJob(ctx, job); err != nil {
		return nil, err
	}

	if s.jobCreated != nil {
		select {
		case s.jobCreated <- struct{}{}:
		default:
		}
	}

	return job, nil
}

func (s *revertService) GetRevertJob(ctx context.Context, id uuid.UUID) (*domain.RevertJob, error) {
	job, err := s.repo.GetJob(ctx, id)
	if err != nil {
		return nil, err
	}
	if job == nil {
		return nil, errors.New("revert job not found")
	}
	return job, nil
}

func (s *revertService) ListRevertJobs(ctx context.Context, page, pageSize int) (*domain.PaginatedResponse[domain.RevertJob], error) {
	if page <= 0 {
		page = 1
	}
	if pageSize <= 0 {
		pageSize = 20
	}
	if pageSize > 100 {
		pageSize = 100
	}

	totalCount, err := s.repo.CountJobs(ctx)
	if err != nil {
		return nil, err
	}

	offset := (page - 1) * pageSize
	jobs, err := s.repo.ListJobs(ctx, offset, pageSize)
	if err != nil {
		return nil, err
	}
	if jobs == nil {
		jobs = []domain.RevertJob{}
	}

	totalPages := (totalCount + pageSize - 1) / pageSize

	return &domain.PaginatedResponse[domain.RevertJob]{
		Data:       jobs,
		Page:       page,
		PageSize:   pageSize,
		TotalCount: totalCount,
		TotalPages: totalPages,
	}, nil
}

func (s *revertService) CancelRevertJob(ctx context.Context, id uuid.UUID) (*domain.RevertJob, error) {
	job, err := s.repo.GetJob(ctx, id)
	if err != nil {
		return nil, err
	}
	if job == nil {
		return nil, errors.New("revert job not found")
	}

	if !job.CanCancel() {
		return nil, errors.New("revert job cannot be cancelled in its current state")
	}

	if job.Status == domain.RevertJobStatusInProgress {
		job.Status = domain.RevertJobStatusCancelling
	} else {
		job.Status = domain.RevertJobStatusCancelled
	}

	job.UpdatedAt = time.Now()
	if err := s.repo.UpdateJob(ctx, job); err != nil {
		return nil, err
	}

	return job, nil
}
