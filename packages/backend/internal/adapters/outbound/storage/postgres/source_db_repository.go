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

type SourceDatabaseEntity struct {
	ID        uuid.UUID `db:"id"`
	UserID    uuid.UUID `db:"user_id"`
	Name      string    `db:"name"`
	Host      string    `db:"host"`
	Port      int       `db:"port"`
	DBName    string    `db:"dbname"`
	Username  string    `db:"username"`
	Password  []byte    `db:"password"`
	SSLMode   string    `db:"sslmode"`
	CreatedAt time.Time `db:"created_at"`
	UpdatedAt time.Time `db:"updated_at"`
}

func (e *SourceDatabaseEntity) ToDomain() *domain.SourceDatabase {
	return &domain.SourceDatabase{
		ID:        e.ID,
		UserID:    e.UserID,
		Name:      e.Name,
		Host:      e.Host,
		Port:      e.Port,
		DBName:    e.DBName,
		Username:  e.Username,
		Password:  e.Password,
		SSLMode:   e.SSLMode,
		CreatedAt: e.CreatedAt,
		UpdatedAt: e.UpdatedAt,
	}
}

type sourceDatabaseRepository struct {
	db *sqlx.DB
}

func NewSourceDatabaseRepository(db *sqlx.DB) outbound.SourceDatabaseRepository {
	return &sourceDatabaseRepository{db: db}
}

func (r *sourceDatabaseRepository) Save(ctx context.Context, db *domain.SourceDatabase) error {
	const query = `
		INSERT INTO source_databases (id, user_id, name, host, port, dbname, username, password, sslmode, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`
	_, err := r.db.ExecContext(ctx, query,
		db.ID, db.UserID, db.Name, db.Host, db.Port, db.DBName, db.Username, db.Password, db.SSLMode, db.CreatedAt, db.UpdatedAt)
	if err != nil {
		return fmt.Errorf("failed to insert source database: %w", err)
	}
	return nil
}

func (r *sourceDatabaseRepository) FindByID(ctx context.Context, id uuid.UUID) (*domain.SourceDatabase, error) {
	var entity SourceDatabaseEntity
	const query = `
		SELECT id, user_id, name, host, port, dbname, username, password, sslmode, created_at, updated_at
		FROM source_databases
		WHERE id = $1`
	if err := r.db.GetContext(ctx, &entity, query, id); err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("source database %s not found", id)
		}
		return nil, fmt.Errorf("failed to find source database: %w", err)
	}
	return entity.ToDomain(), nil
}

func (r *sourceDatabaseRepository) FindAllByUser(ctx context.Context, userID uuid.UUID) ([]domain.SourceDatabase, error) {
	var entities []SourceDatabaseEntity
	const query = `
		SELECT id, user_id, name, host, port, dbname, username, password, sslmode, created_at, updated_at
		FROM source_databases
		WHERE user_id = $1
		ORDER BY created_at DESC`
	if err := r.db.SelectContext(ctx, &entities, query, userID); err != nil {
		return nil, fmt.Errorf("failed to list source databases: %w", err)
	}
	result := make([]domain.SourceDatabase, len(entities))
	for i, e := range entities {
		result[i] = *e.ToDomain()
	}
	return result, nil
}

func (r *sourceDatabaseRepository) Update(ctx context.Context, db *domain.SourceDatabase) error {
	const query = `
		UPDATE source_databases
		SET name = $2, host = $3, port = $4, dbname = $5, username = $6, password = $7, sslmode = $8, updated_at = $9
		WHERE id = $1`
	result, err := r.db.ExecContext(ctx, query,
		db.ID, db.Name, db.Host, db.Port, db.DBName, db.Username, db.Password, db.SSLMode, db.UpdatedAt)
	if err != nil {
		return fmt.Errorf("failed to update source database: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}
	if rows == 0 {
		return fmt.Errorf("source database %s not found", db.ID)
	}
	return nil
}

func (r *sourceDatabaseRepository) Delete(ctx context.Context, id uuid.UUID) error {
	const query = `DELETE FROM source_databases WHERE id = $1`
	result, err := r.db.ExecContext(ctx, query, id)
	if err != nil {
		return fmt.Errorf("failed to delete source database: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}
	if rows == 0 {
		return fmt.Errorf("source database %s not found", id)
	}
	return nil
}
