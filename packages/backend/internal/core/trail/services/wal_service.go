package services

import (
	"context"
	"fmt"
	"math"

	"trail-replay/internal/core/trail/domain"
	"trail-replay/internal/core/trail/ports/inbound"
	"trail-replay/internal/core/trail/ports/outbound"
)

const (
	defaultPageSize = 20
	maxPageSize     = 100
)

type walQueryService struct {
	repo outbound.WalQueryRepository
}

func NewWalQueryService(repo outbound.WalQueryRepository) inbound.WalQueryService {
	return &walQueryService{repo: repo}
}

func (s *walQueryService) ListWalTransactions(ctx context.Context, page, pageSize int) (*domain.PaginatedResponse[domain.WalTransactionResult], error) {
	if s.repo == nil {
		return nil, fmt.Errorf("wal query service is not available")
	}

	if page < 1 {
		page = 1
	}
	if pageSize < 1 {
		pageSize = defaultPageSize
	}
	if pageSize > maxPageSize {
		pageSize = maxPageSize
	}

	result, err := s.repo.ListWalTransactionsPaginated(ctx, page, pageSize)
	if err != nil {
		return nil, fmt.Errorf("list wal transactions: %w", err)
	}

	result.TotalPages = int(math.Ceil(float64(result.TotalCount) / float64(pageSize)))
	result.Page = page
	result.PageSize = pageSize

	return result, nil
}
