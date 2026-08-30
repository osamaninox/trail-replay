package services

import (
	"context"
	"time"

	"trail-replay/internal/core/trail/domain"
	"trail-replay/internal/core/trail/ports/outbound"
)

const batchSize = 100

func StartRevertJobRunner(ctx context.Context, repo outbound.RevertRepository, executor outbound.SourceDBExecutor, jobCreated <-chan struct{}) {
	go func() {
		tryProcessBatch(ctx, repo, executor)

		for {
			select {
			case <-ctx.Done():
				return
			case <-jobCreated:
				tryProcessBatch(ctx, repo, executor)
			}
		}
	}()
}

func tryProcessBatch(ctx context.Context, repo outbound.RevertRepository, executor outbound.SourceDBExecutor) {
	locked, err := repo.TryAcquireAdvisoryLock(ctx)
	if err != nil || !locked {
		return
	}

	processAvailableJobs(ctx, repo, executor)

	repo.ReleaseAdvisoryLock(ctx)
}

func processAvailableJobs(ctx context.Context, repo outbound.RevertRepository, executor outbound.SourceDBExecutor) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		job, err := repo.GetActiveJob(ctx)
		if err != nil {
			return
		}
		if job == nil {
			job, err = repo.GetNextPendingJob(ctx)
			if err != nil {
				return
			}
			if job == nil {
				return
			}

			total, err := repo.CountUnappliedChanges(ctx, job.InputFrom, job.InputTo)
			if err != nil {
				return
			}
			job.TotalChanges = total
		}

		if job.Status == domain.RevertJobStatusPending {
			job.Status = domain.RevertJobStatusInProgress
		}
		job.UpdatedAt = time.Now()
		if err := repo.UpdateJob(ctx, job); err != nil {
			return
		}

		offset := job.CompletedCount + job.FailedCount
		for offset < job.TotalChanges {
			select {
			case <-ctx.Done():
				return
			default:
			}

			currentJob, err := repo.GetJob(ctx, job.ID)
			if err != nil {
				return
			}
			if currentJob.Status == domain.RevertJobStatusCancelling {
				currentJob.Status = domain.RevertJobStatusCancelled
				currentJob.UpdatedAt = time.Now()
				now := time.Now()
				currentJob.CompletedAt = &now
				repo.UpdateJob(ctx, currentJob)
				return
			}

			changes, err := repo.FetchUnappliedChanges(ctx, job.InputFrom, job.InputTo, offset, batchSize)
			if err != nil {
				job.Status = domain.RevertJobStatusFailed
				job.LastError = err.Error()
				job.UpdatedAt = time.Now()
				now := time.Now()
				job.CompletedAt = &now
				repo.UpdateJob(ctx, job)
				return
			}
			if len(changes) == 0 {
				break
			}

			for _, ch := range changes {
				if err := executor.Exec(ctx, ch.ReverseDMLSQL); err != nil {
					job.FailedCount++
					repo.MarkChangeFailed(ctx, job.ID, ch, err.Error())
				} else {
					job.CompletedCount++
					repo.MarkChangeApplied(ctx, job.ID, ch)
				}
			}

			offset += len(changes)
			job.UpdatedAt = time.Now()
			if err := repo.UpdateJob(ctx, job); err != nil {
				return
			}
		}

		if job.FailedCount > 0 {
			job.Status = domain.RevertJobStatusFailed
			job.LastError = "some changes failed to revert"
		} else {
			job.Status = domain.RevertJobStatusCompleted
		}
		job.UpdatedAt = time.Now()
		now := time.Now()
		job.CompletedAt = &now
		repo.UpdateJob(ctx, job)
	}
}
