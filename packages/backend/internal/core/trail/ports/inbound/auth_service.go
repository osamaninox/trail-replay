package inbound

import (
	"context"

	"github.com/google/uuid"

	"trail-replay/internal/core/trail/domain"
)

// AuthService is the driving port for authentication operations.
type AuthService interface {
	Signup(ctx context.Context, email, password string) (*domain.AuthResult, error)
	Login(ctx context.Context, email, password string) (*domain.AuthResult, error)
	ValidateToken(token string) (uuid.UUID, error)
}
