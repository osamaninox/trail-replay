package services_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/osamakhalid/trail-replay/internal/core/trail/domain"
	"github.com/osamakhalid/trail-replay/internal/core/trail/services"
)

type mockWalQueryRepository struct {
	data       *domain.PaginatedResponse[domain.WalTransactionResult]
	err        error
	lastPage   int
	lastSize   int
}

func (m *mockWalQueryRepository) ListWalTransactionsPaginated(ctx context.Context, page, pageSize int) (*domain.PaginatedResponse[domain.WalTransactionResult], error) {
	m.lastPage = page
	m.lastSize = pageSize
	if m.err != nil {
		return nil, m.err
	}
	return m.data, nil
}

func TestListWalTransactions_NilRepo(t *testing.T) {
	svc := services.NewWalQueryService(nil)
	_, err := svc.ListWalTransactions(context.Background(), 1, 20)
	if err == nil {
		t.Fatal("expected error for nil repo")
	}
}

func TestListWalTransactions_Defaults(t *testing.T) {
	mock := &mockWalQueryRepository{
		data: &domain.PaginatedResponse[domain.WalTransactionResult]{
			Data:       []domain.WalTransactionResult{},
			TotalCount: 0,
		},
	}
	svc := services.NewWalQueryService(mock)

	result, err := svc.ListWalTransactions(context.Background(), 0, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Page != 1 {
		t.Errorf("expected page=1, got %d", result.Page)
	}
	if result.PageSize != 20 {
		t.Errorf("expected pageSize=20, got %d", result.PageSize)
	}
	if mock.lastPage != 1 {
		t.Errorf("repo called with page=%d, want 1", mock.lastPage)
	}
	if mock.lastSize != 20 {
		t.Errorf("repo called with pageSize=%d, want 20", mock.lastSize)
	}
}

func TestListWalTransactions_MaxPageSize(t *testing.T) {
	mock := &mockWalQueryRepository{
		data: &domain.PaginatedResponse[domain.WalTransactionResult]{
			Data:       []domain.WalTransactionResult{},
			TotalCount: 0,
		},
	}
	svc := services.NewWalQueryService(mock)

	result, err := svc.ListWalTransactions(context.Background(), 1, 200)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.PageSize != 100 {
		t.Errorf("expected pageSize=100, got %d", result.PageSize)
	}
	if mock.lastSize != 100 {
		t.Errorf("repo called with pageSize=%d, want 100", mock.lastSize)
	}
}

func TestListWalTransactions_RepoError(t *testing.T) {
	repoErr := errors.New("db connection failed")
	mock := &mockWalQueryRepository{err: repoErr}
	svc := services.NewWalQueryService(mock)

	_, err := svc.ListWalTransactions(context.Background(), 1, 20)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestListWalTransactions_SetsPaginationMeta(t *testing.T) {
	now := time.Now()
	mock := &mockWalQueryRepository{
		data: &domain.PaginatedResponse[domain.WalTransactionResult]{
			Data: []domain.WalTransactionResult{
				{
					ID:          1,
					SourceSlot:  "trail_replay_poc_slot",
					SourceDb:    "traildb",
					Xid:         12345,
					CommitTS:    now,
					ChangeCount: 2,
					IngestedAt:  now,
					Changes: []domain.WalChangeResult{
						{
							ID:             1,
							TransactionID:  1,
							ChangeSeqInTxn: 1,
							SchemaName:     "public",
							TableName:      "users",
							Op:             "U",
							ChangedColumns: []string{"email"},
							ForwardDMLSQL:  "UPDATE users SET email='new' WHERE id=1",
							ReverseDMLSQL:  "UPDATE users SET email='old' WHERE id=1",
							UndoStatus:     "pending",
							CreatedAt:      now,
						},
					},
				},
			},
			TotalCount: 150,
		},
	}
	svc := services.NewWalQueryService(mock)

	result, err := svc.ListWalTransactions(context.Background(), 1, 20)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Page != 1 {
		t.Errorf("expected page=1, got %d", result.Page)
	}
	if result.PageSize != 20 {
		t.Errorf("expected pageSize=20, got %d", result.PageSize)
	}
	if result.TotalCount != 150 {
		t.Errorf("expected TotalCount=150, got %d", result.TotalCount)
	}
	if result.TotalPages != 8 {
		t.Errorf("expected TotalPages=8 (150/20=7.5 ceil=8), got %d", result.TotalPages)
	}
	if len(result.Data) != 1 {
		t.Fatalf("expected 1 transaction, got %d", len(result.Data))
	}
	txn := result.Data[0]
	if txn.ID != 1 {
		t.Errorf("expected txn ID=1, got %d", txn.ID)
	}
	if len(txn.Changes) != 1 {
		t.Fatalf("expected 1 change, got %d", len(txn.Changes))
	}
	if txn.Changes[0].TableName != "users" {
		t.Errorf("expected change table=users, got %s", txn.Changes[0].TableName)
	}
}

func TestListWalTransactions_ZeroTotalPages(t *testing.T) {
	mock := &mockWalQueryRepository{
		data: &domain.PaginatedResponse[domain.WalTransactionResult]{
			Data:       []domain.WalTransactionResult{},
			TotalCount: 0,
		},
	}
	svc := services.NewWalQueryService(mock)

	result, err := svc.ListWalTransactions(context.Background(), 1, 20)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.TotalPages != 0 {
		t.Errorf("expected TotalPages=0 for empty results, got %d", result.TotalPages)
	}
	if len(result.Data) != 0 {
		t.Errorf("expected 0 transactions, got %d", len(result.Data))
	}
}

func TestListWalTransactions_PageSizeExact(t *testing.T) {
	mock := &mockWalQueryRepository{
		data: &domain.PaginatedResponse[domain.WalTransactionResult]{
			Data:       []domain.WalTransactionResult{},
			TotalCount: 100,
		},
	}
	svc := services.NewWalQueryService(mock)

	result, err := svc.ListWalTransactions(context.Background(), 2, 20)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Page != 2 {
		t.Errorf("expected page=2, got %d", result.Page)
	}
	if result.TotalPages != 5 {
		t.Errorf("expected TotalPages=5 (100/20), got %d", result.TotalPages)
	}
}
