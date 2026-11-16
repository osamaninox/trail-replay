package services

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"trail-replay/internal/core/trail/domain"
	"trail-replay/internal/core/trail/ports/inbound"
	"trail-replay/internal/core/trail/ports/outbound"
)

type revertService struct {
	repo      outbound.RevertRepository
	submitter outbound.RevertJobSubmitter
	executor  outbound.SourceDBExecutor
}

func NewRevertService(repo outbound.RevertRepository, submitter outbound.RevertJobSubmitter, executor outbound.SourceDBExecutor) inbound.RevertService {
	return &revertService{repo: repo, submitter: submitter, executor: executor}
}

func (s *revertService) CreateRevertJob(ctx context.Context, from, to time.Time) (*domain.RevertJob, error) {
	if !from.Before(to) {
		return nil, errors.New("from must be before to")
	}

	// Estimate only: the workflow's plan step freezes the authoritative work
	// list (and change-level total) at execution time.
	total, err := s.repo.CountRevertableTransactions(ctx, from, to)
	if err != nil {
		return nil, err
	}

	job := domain.NewRevertJob(from, to)
	job.TotalChanges = total
	if err := s.repo.CreateJob(ctx, job); err != nil {
		return nil, err
	}

	// If Submit fails, the pending row stays for startup reconciliation (§4).
	if err := s.submitter.Submit(ctx, job.ID); err != nil {
		return nil, err
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

// PreviewRevert builds the dry run: the window's changes in revert order are
// handed to the executor (which rolls back), and the per-statement results
// are aggregated into one current → reverted verdict per affected row. A row
// touched several times in the window reports only its net effect (e.g.
// inserted then updated reverts to "delete").
func (s *revertService) PreviewRevert(ctx context.Context, from, to time.Time) (*domain.RevertPreview, error) {
	if !from.Before(to) {
		return nil, errors.New("from must be before to")
	}
	if s.executor == nil {
		return nil, errors.New("preview unavailable: source database not configured")
	}

	changes, err := s.repo.ListRevertableChanges(ctx, from, to)
	if err != nil {
		return nil, err
	}

	stmts := make([]outbound.PreviewStatement, 0, len(changes))
	kept := make([]domain.RevertChange, 0, len(changes))
	skipped := 0
	for _, ch := range changes {
		if strings.TrimSpace(ch.ReverseDMLSQL) == "" {
			skipped++ // e.g. no REPLICA IDENTITY FULL — nothing to revert with
			continue
		}
		row := ch.NewRow
		if ch.Op == "D" {
			row = ch.OldRow
		}
		stmts = append(stmts, outbound.PreviewStatement{
			SQL:    ch.ReverseDMLSQL,
			Schema: ch.SchemaName,
			Table:  ch.TableName,
			Op:     ch.Op,
			Row:    row,
		})
		kept = append(kept, ch)
	}

	results, err := s.executor.PreviewTx(ctx, stmts)
	if err != nil {
		return nil, err
	}

	preview := aggregatePreview(from, to, changes, kept, results)
	preview.Summary.SkippedChanges = skipped
	return preview, nil
}

// rowAgg accumulates the per-row verdict across every statement touching it.
// final==nil with resolved==true means "row absent after the revert".
type rowAgg struct {
	table      string
	pk         map[string]any
	current    map[string]any
	haveRow    bool
	final      map[string]any
	resolved   bool
	conflict   *domain.PreviewConflict
	warnings   map[string]bool
	changed    map[string]bool
	changedOrd []string
	txns       []uint64
	txnSeen    map[uint64]bool
}

func aggregatePreview(from, to time.Time, all []domain.RevertChange, kept []domain.RevertChange, results []outbound.PreviewStatementResult) *domain.RevertPreview {
	txnSet := map[uint64]bool{}
	for _, ch := range all {
		txnSet[ch.TransactionID] = true
	}

	var tableOrder []string
	tables := map[string][]*rowAgg{}
	rowByKey := map[string]*rowAgg{}

	getRow := func(table string, res outbound.PreviewStatementResult, ref map[string]any) *rowAgg {
		pk := res.PK
		if pk == nil {
			pk = ref // no resolvable PK — key by the payload itself
		}
		keyData, _ := json.Marshal(pk)
		key := table + "|" + string(keyData)
		if agg, ok := rowByKey[key]; ok {
			return agg
		}
		agg := &rowAgg{
			table:    table,
			pk:       res.PK,
			warnings: map[string]bool{},
			changed:  map[string]bool{},
			txnSeen:  map[uint64]bool{},
		}
		rowByKey[key] = agg
		if _, ok := tables[table]; !ok {
			tableOrder = append(tableOrder, table)
		}
		tables[table] = append(tables[table], agg)
		return agg
	}

	for i, ch := range kept {
		res := results[i]
		table := ch.SchemaName + "." + ch.TableName
		agg := getRow(table, res, ch.NewRow)

		if !agg.haveRow {
			agg.current = res.Before
			agg.haveRow = true
		}
		if !agg.txnSeen[ch.TransactionID] {
			agg.txnSeen[ch.TransactionID] = true
			agg.txns = append(agg.txns, ch.TransactionID)
		}
		for _, col := range ch.ChangedColumns {
			if !agg.changed[col] {
				agg.changed[col] = true
				agg.changedOrd = append(agg.changedOrd, col)
			}
		}

		if res.Conflict != nil {
			agg.conflict = res.Conflict // keep the latest failure for this row
			continue
		}

		switch ch.Op {
		case "I": // reverse DELETE — row removed when it matched
			if len(res.Returned) > 0 {
				agg.final, agg.resolved = nil, true
			} else {
				agg.warnings["stale"] = true
			}
		case "D": // reverse INSERT — row restored when it matched
			if len(res.Returned) > 0 {
				agg.final, agg.resolved = res.Returned[0], true
			} else {
				// ON CONFLICT DO NOTHING: the row is already back.
				agg.warnings["noop"] = true
				if !agg.resolved {
					agg.final, agg.resolved = res.Before, true
				}
			}
		case "U": // reverse UPDATE — row restored when it matched
			if len(res.Returned) > 0 {
				agg.final, agg.resolved = res.Returned[0], true
			} else {
				agg.warnings["stale"] = true
			}
		}
	}

	sort.Strings(tableOrder)
	preview := &domain.RevertPreview{
		From: from,
		To:   to,
		Summary: domain.PreviewSummary{
			Transactions: len(txnSet),
			Changes:      len(all),
		},
		Tables: []domain.PreviewTable{},
	}
	for _, table := range tableOrder {
		pt := domain.PreviewTable{Table: table, Rows: []domain.PreviewRow{}}
		for _, agg := range tables[table] {
			row := agg.toRow()
			pt.Rows = append(pt.Rows, row)
			switch row.Effect {
			case domain.PreviewEffectUpdate, domain.PreviewEffectRestore, domain.PreviewEffectDelete:
				preview.Summary.RowsAffected++
			case domain.PreviewEffectConflict:
				preview.Summary.Conflicts++
			case domain.PreviewEffectStale:
				preview.Summary.Stale++
			case domain.PreviewEffectNoop:
				preview.Summary.Noop++
			}
		}
		preview.Tables = append(preview.Tables, pt)
	}
	return preview
}

func (agg *rowAgg) toRow() domain.PreviewRow {
	effect := domain.PreviewEffectNoop
	switch {
	case agg.conflict != nil:
		effect = domain.PreviewEffectConflict
	case !agg.resolved:
		effect = domain.PreviewEffectStale // every statement matched nothing
	case agg.current == nil && agg.final != nil:
		effect = domain.PreviewEffectRestore
	case agg.current != nil && agg.final == nil:
		effect = domain.PreviewEffectDelete
	case agg.current != nil && agg.final != nil:
		effect = domain.PreviewEffectUpdate
	}

	pk := agg.pk
	if pk == nil {
		pk = map[string]any{}
	}
	var warnings []string
	for w := range agg.warnings {
		warnings = append(warnings, w)
	}
	sort.Strings(warnings)

	return domain.PreviewRow{
		Table:          agg.table,
		PK:             pk,
		Effect:         effect,
		Current:        agg.current,
		Reverted:       agg.final,
		ChangedColumns: agg.changedOrd,
		Transactions:   agg.txns,
		Conflict:       agg.conflict,
		Warnings:       warnings,
	}
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

func (s *revertService) ListJobTransactions(ctx context.Context, jobID uuid.UUID, page, pageSize int) (*domain.PaginatedResponse[domain.RevertJobTransaction], error) {
	job, err := s.repo.GetJob(ctx, jobID)
	if err != nil {
		return nil, err
	}
	if job == nil {
		return nil, errors.New("revert job not found")
	}

	if page <= 0 {
		page = 1
	}
	if pageSize <= 0 {
		pageSize = 20
	}
	if pageSize > 100 {
		pageSize = 100
	}

	totalCount, err := s.repo.CountJobTransactions(ctx, jobID)
	if err != nil {
		return nil, err
	}

	offset := (page - 1) * pageSize
	transactions, err := s.repo.ListJobTransactions(ctx, jobID, offset, pageSize)
	if err != nil {
		return nil, err
	}
	if transactions == nil {
		transactions = []domain.RevertJobTransaction{}
	}

	totalPages := (totalCount + pageSize - 1) / pageSize

	return &domain.PaginatedResponse[domain.RevertJobTransaction]{
		Data:       transactions,
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

	// Cancelled immediately (plan §3): the in-flight txn finishes, remaining
	// steps abort at the next step boundary. No 'cancelling' interim state.
	job.Status = domain.RevertJobStatusCancelled
	job.UpdatedAt = time.Now()
	if err := s.repo.UpdateJob(ctx, job); err != nil {
		return nil, err
	}

	if err := s.submitter.Cancel(ctx, id); err != nil {
		return nil, err
	}

	return job, nil
}
