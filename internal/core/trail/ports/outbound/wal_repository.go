package outbound

import (
	"context"

	"github.com/osamakhalid/trail-replay/internal/core/trail/domain"
)

type WalQueryRepository interface {
	ListWalTransactionsPaginated(ctx context.Context, page, pageSize int) (*domain.PaginatedResponse[domain.WalTransactionResult], error)
}
