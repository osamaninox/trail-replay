package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"

	"trail-replay/internal/core/trail/domain"
	"trail-replay/internal/core/trail/ports/outbound"
)

type UserEntity struct {
	ID           uuid.UUID `db:"id"`
	Email        string    `db:"email"`
	PasswordHash string    `db:"password"`
	CreatedAt    time.Time `db:"created_at"`
}

func (e *UserEntity) ToDomain() *domain.User {
	return &domain.User{
		ID:           e.ID,
		Email:        e.Email,
		PasswordHash: e.PasswordHash,
		CreatedAt:    e.CreatedAt,
	}
}

type userRepository struct {
	db *sqlx.DB
}

func NewUserRepository(db *sqlx.DB) outbound.UserRepository {
	return &userRepository{db: db}
}

func (r *userRepository) Save(ctx context.Context, user *domain.User) error {
	const query = `
		INSERT INTO users (id, email, password, created_at)
		VALUES ($1, $2, $3, $4)`
	_, err := r.db.ExecContext(ctx, query, user.ID, user.Email, user.PasswordHash, user.CreatedAt)
	if err != nil {
		return fmt.Errorf("failed to insert user: %w", err)
	}
	return nil
}

func (r *userRepository) FindByEmail(ctx context.Context, email string) (*domain.User, error) {
	var entity UserEntity
	const query = `
		SELECT id, email, password, created_at
		FROM users
		WHERE email = $1`
	if err := r.db.GetContext(ctx, &entity, query, email); err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("user with email %s not found", email)
		}
		return nil, fmt.Errorf("failed to find user: %w", err)
	}
	return entity.ToDomain(), nil
}

func (r *userRepository) FindByID(ctx context.Context, id uuid.UUID) (*domain.User, error) {
	var entity UserEntity
	const query = `
		SELECT id, email, password, created_at
		FROM users
		WHERE id = $1`
	if err := r.db.GetContext(ctx, &entity, query, id); err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("user %s not found", id)
		}
		return nil, fmt.Errorf("failed to find user: %w", err)
	}
	return entity.ToDomain(), nil
}
