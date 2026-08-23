package outbound

import (
	"context"

	"trail-replay/internal/core/trail/domain"
)

type WalQueryRepository interface {
	ListWalTransactionsPaginated(ctx context.Context, page, pageSize int) (*domain.PaginatedResponse[domain.WalTransactionResult], error)
}
