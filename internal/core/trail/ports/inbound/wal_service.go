package inbound

import (
	"context"

	"github.com/osamakhalid/trail-replay/internal/core/trail/domain"
)

type WalQueryService interface {
	ListWalTransactions(ctx context.Context, page, pageSize int) (*domain.PaginatedResponse[domain.WalTransactionResult], error)
}
