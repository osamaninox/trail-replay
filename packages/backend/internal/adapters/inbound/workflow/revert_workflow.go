// Package workflow adapts the revert engine to the DBOS durable-execution
// runtime. The workflow, its queue, and the RevertJobSubmitter implementation
// live here so DBOS never leaks into the core.
package workflow

import (
	"context"
	"fmt"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos"
	"github.com/google/uuid"

	"trail-replay/internal/core/trail/domain"
	"trail-replay/internal/core/trail/ports/outbound"
)

// QueueName serializes revert jobs: one transaction-granular plan at a time,
// so plan snapshots never overlap (plan decision #15).
const QueueName = "revert-jobs"

// planResult is the checkpointed output of the plan step (step 0).
type planResult struct {
	Count     int  `json:"count"`
	Cancelled bool `json:"cancelled"`
}

// RevertWorkflow is the DBOS-backed revert engine and RevertJobSubmitter.
// Workflow ID = job ID (decision #10), so Submit is idempotent and Cancel
// addresses the running execution directly.
type RevertWorkflow struct {
	repo     outbound.RevertRepository
	executor outbound.SourceDBExecutor
	walDS    *dbos.DataSource
	dbosCtx  dbos.Context
	queue    dbos.Queue
}

// NewRevertWorkflow registers the WAL database as a durable-transaction data
// source, registers the queue and workflow, and returns the ready submitter.
// walDB is the same database DBOS uses as its system DB (decision #17).
func NewRevertWorkflow(
	ctx dbos.Context,
	repo outbound.RevertRepository,
	executor outbound.SourceDBExecutor,
	walDS *dbos.DataSource,
) (*RevertWorkflow, error) {
	queue, err := dbos.RegisterQueue(ctx, QueueName, dbos.WithWorkerConcurrency(1))
	if err != nil {
		return nil, fmt.Errorf("register queue: %w", err)
	}

	w := &RevertWorkflow{
		repo:     repo,
		executor: executor,
		walDS:    walDS,
		dbosCtx:  ctx,
		queue:    queue,
	}
	dbos.RegisterWorkflow(ctx, w.run)
	return w, nil
}

// Submit enqueues the job's workflow. Idempotent: a second Submit with the
// same job ID returns the existing workflow instead of a duplicate run
// (startup reconciliation relies on this, plan §4).
func (w *RevertWorkflow) Submit(ctx context.Context, jobID uuid.UUID) error {
	_, err := dbos.RunWorkflow(w.dbosCtx, w.run, jobID.String(),
		dbos.WithWorkflowID(jobID.String()),
		dbos.WithQueue(w.queue),
	)
	if err != nil {
		return fmt.Errorf("enqueue revert workflow: %w", err)
	}
	return nil
}

// Cancel cancels the workflow execution. The in-flight transaction finishes;
// the loop aborts at the next step boundary via context cancellation.
func (w *RevertWorkflow) Cancel(ctx context.Context, jobID uuid.UUID) error {
	if err := w.dbosCtx.CancelWorkflow(w.dbosCtx, jobID.String()); err != nil {
		return fmt.Errorf("cancel revert workflow: %w", err)
	}
	return nil
}

// run is the durable workflow. Steps replay by index (decision #11): step 0
// is the plan; iteration i's exec/mark steps revert the transaction at seq=i.
func (w *RevertWorkflow) run(ctx dbos.Context, jobIDStr string) (string, error) {
	jobID, err := uuid.Parse(jobIDStr)
	if err != nil {
		return "", fmt.Errorf("invalid job id %q: %w", jobIDStr, err)
	}

	plan, err := dbos.RunAsStep(ctx, func(ctx context.Context) (planResult, error) {
		return w.planStep(ctx, jobID)
	}, dbos.WithStepName("plan"))
	if err != nil {
		return "", fmt.Errorf("plan step: %w", err)
	}
	if plan.Cancelled {
		return string(domain.RevertJobStatusCancelled), nil
	}

	for i := 1; i <= plan.Count; i++ {
		if ctx.Err() != nil {
			// Job row already marked cancelled by the service.
			return string(domain.RevertJobStatusCancelled), nil
		}

		_, execErr := dbos.RunAsStep(ctx, func(ctx context.Context) (any, error) {
			return nil, w.execStep(ctx, jobID, i)
		},
			dbos.WithStepName(fmt.Sprintf("revert-txn-%d-exec", i)),
			dbos.WithStepMaxRetries(3),
			dbos.WithStepBaseInterval(time.Second),
			dbos.WithStepBackoffFactor(2),
			dbos.WithStepMaxInterval(30*time.Second),
		)

		if execErr != nil {
			_, markErr := dbos.RunAsTransaction(ctx, w.walDS, func(ctx context.Context, tx dbos.Tx) (any, error) {
				return nil, w.repo.MarkTransactionFailed(ctx, dbosSQLTx{tx}, jobID, i, execErr.Error())
			}, dbos.WithStepName(fmt.Sprintf("revert-txn-%d-mark-failed", i)))
			if markErr != nil {
				return "", fmt.Errorf("mark-failed step %d: %w", i, markErr)
			}
			continue
		}

		_, err := dbos.RunAsTransaction(ctx, w.walDS, func(ctx context.Context, tx dbos.Tx) (any, error) {
			return nil, w.repo.MarkTransactionApplied(ctx, dbosSQLTx{tx}, jobID, i)
		}, dbos.WithStepName(fmt.Sprintf("revert-txn-%d-mark", i)))
		if err != nil {
			return "", fmt.Errorf("mark step %d: %w", i, err)
		}
	}

	if ctx.Err() != nil {
		return string(domain.RevertJobStatusCancelled), nil
	}

	status, err := dbos.RunAsStep(ctx, func(ctx context.Context) (string, error) {
		return w.finalizeStep(ctx, jobID)
	}, dbos.WithStepName("finalize"))
	if err != nil {
		return "", fmt.Errorf("finalize step: %w", err)
	}
	return status, nil
}

// planStep freezes the work list (idempotently — replay after a lost
// checkpoint is a no-op) and moves the job to in_progress with the
// authoritative change-level total.
func (w *RevertWorkflow) planStep(ctx context.Context, jobID uuid.UUID) (planResult, error) {
	job, err := w.repo.GetJob(ctx, jobID)
	if err != nil {
		return planResult{}, err
	}
	if job == nil {
		return planResult{}, fmt.Errorf("revert job %s not found", jobID)
	}
	if job.Status == domain.RevertJobStatusCancelled {
		return planResult{Cancelled: true}, nil
	}

	count, totalChanges, err := w.repo.SnapshotPlan(ctx, jobID, job.InputFrom, job.InputTo)
	if err != nil {
		return planResult{}, err
	}

	job.Status = domain.RevertJobStatusInProgress
	job.TotalChanges = totalChanges
	job.UpdatedAt = time.Now()
	if err := w.repo.UpdateJob(ctx, job); err != nil {
		return planResult{}, err
	}
	return planResult{Count: count}, nil
}

// execStep applies one transaction's reverse DMLs atomically on the source
// database. Idempotent reverse SQL (plan §5) makes re-execution safe; the
// status check is belt-and-suspenders for the WAL-mark-committed window.
func (w *RevertWorkflow) execStep(ctx context.Context, jobID uuid.UUID, seq int) error {
	row, err := w.repo.GetJobTransaction(ctx, jobID, seq)
	if err != nil {
		return err
	}
	if row == nil {
		return fmt.Errorf("no plan row for seq %d", seq)
	}
	if row.Status == domain.RevertTransactionStatusApplied {
		return nil
	}

	changes, err := w.repo.GetTransactionChanges(ctx, row.TransactionID)
	if err != nil {
		return err
	}
	stmts := make([]string, 0, len(changes))
	for _, ch := range changes {
		stmts = append(stmts, ch.ReverseDMLSQL)
	}
	return w.executor.ExecTx(ctx, stmts)
}

// finalizeStep settles the job: completed when nothing failed, failed
// otherwise. Cancelled jobs are left alone (the service owns that transition).
func (w *RevertWorkflow) finalizeStep(ctx context.Context, jobID uuid.UUID) (string, error) {
	job, err := w.repo.GetJob(ctx, jobID)
	if err != nil {
		return "", err
	}
	if job == nil {
		return "", fmt.Errorf("revert job %s not found", jobID)
	}

	now := time.Now()
	if job.FailedCount > 0 {
		job.Status = domain.RevertJobStatusFailed
		job.LastError = "some transactions failed to revert"
	} else {
		job.Status = domain.RevertJobStatusCompleted
	}
	job.UpdatedAt = now
	job.CompletedAt = &now
	if err := w.repo.UpdateJob(ctx, job); err != nil {
		return "", err
	}
	return string(job.Status), nil
}

// dbosSQLTx adapts DBOS's durable transaction handle to the repository's
// minimal SQLTx surface.
type dbosSQLTx struct{ tx dbos.Tx }

func (a dbosSQLTx) Exec(ctx context.Context, query string, args ...any) error {
	_, err := a.tx.Exec(ctx, query, args...)
	return err
}
