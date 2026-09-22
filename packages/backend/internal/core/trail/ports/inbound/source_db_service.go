package inbound

import (
	"context"

	"github.com/google/uuid"

	"trail-replay/internal/core/trail/domain"
)

// SourceDatabaseInput carries the fields needed to create or update a source
// database configuration. An empty Password on update keeps the existing one.
type SourceDatabaseInput struct {
	Name     string
	Host     string
	Port     int
	DBName   string
	Username string
	Password string
	SSLMode  string
}

// SourceDatabaseService is the driving port for source database configuration.
type SourceDatabaseService interface {
	CreateDatabase(ctx context.Context, userID uuid.UUID, in SourceDatabaseInput) (*domain.SourceDatabase, error)
	GetDatabase(ctx context.Context, userID, id uuid.UUID) (*domain.SourceDatabase, error)
	ListDatabases(ctx context.Context, userID uuid.UUID) ([]domain.SourceDatabase, error)
	UpdateDatabase(ctx context.Context, userID, id uuid.UUID, in SourceDatabaseInput) (*domain.SourceDatabase, error)
	DeleteDatabase(ctx context.Context, userID, id uuid.UUID) error
	TestConnection(ctx context.Context, userID, id uuid.UUID) domain.ConnectionTestResult
}
