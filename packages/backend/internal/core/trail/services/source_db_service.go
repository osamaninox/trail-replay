package services

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/google/uuid"
	_ "github.com/lib/pq"

	"trail-replay/internal/core/trail/domain"
	"trail-replay/internal/core/trail/ports/inbound"
	"trail-replay/internal/core/trail/ports/outbound"
	"trail-replay/pkg/crypto"
)

const testConnectionTimeout = 5 * time.Second

type sourceDatabaseService struct {
	repo      outbound.SourceDatabaseRepository
	encryptor crypto.Encryptor
}

func NewSourceDatabaseService(repo outbound.SourceDatabaseRepository, encryptor crypto.Encryptor) inbound.SourceDatabaseService {
	return &sourceDatabaseService{repo: repo, encryptor: encryptor}
}

func (s *sourceDatabaseService) CreateDatabase(ctx context.Context, userID uuid.UUID, in inbound.SourceDatabaseInput) (*domain.SourceDatabase, error) {
	if err := validateInput(in, true); err != nil {
		return nil, err
	}

	encrypted, err := s.encryptor.Encrypt([]byte(in.Password))
	if err != nil {
		return nil, fmt.Errorf("failed to encrypt password: %w", err)
	}

	now := time.Now()
	db := &domain.SourceDatabase{
		ID:        uuid.New(),
		UserID:    userID,
		Name:      in.Name,
		Host:      in.Host,
		Port:      withDefaultPort(in.Port),
		DBName:    in.DBName,
		Username:  in.Username,
		Password:  encrypted,
		SSLMode:   withDefaultSSLMode(in.SSLMode),
		CreatedAt: now,
		UpdatedAt: now,
	}
	if err := s.repo.Save(ctx, db); err != nil {
		return nil, fmt.Errorf("failed to save source database: %w", err)
	}
	return db, nil
}

func (s *sourceDatabaseService) GetDatabase(ctx context.Context, userID, id uuid.UUID) (*domain.SourceDatabase, error) {
	db, err := s.repo.FindByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if db.UserID != userID {
		return nil, fmt.Errorf("source database %s not found", id)
	}
	return db, nil
}

func (s *sourceDatabaseService) ListDatabases(ctx context.Context, userID uuid.UUID) ([]domain.SourceDatabase, error) {
	return s.repo.FindAllByUser(ctx, userID)
}

func (s *sourceDatabaseService) UpdateDatabase(ctx context.Context, userID, id uuid.UUID, in inbound.SourceDatabaseInput) (*domain.SourceDatabase, error) {
	db, err := s.GetDatabase(ctx, userID, id)
	if err != nil {
		return nil, err
	}
	if err := validateInput(in, false); err != nil {
		return nil, err
	}

	db.Name = in.Name
	db.Host = in.Host
	db.Port = withDefaultPort(in.Port)
	db.DBName = in.DBName
	db.Username = in.Username
	db.SSLMode = withDefaultSSLMode(in.SSLMode)
	db.UpdatedAt = time.Now()

	if in.Password != "" {
		encrypted, err := s.encryptor.Encrypt([]byte(in.Password))
		if err != nil {
			return nil, fmt.Errorf("failed to encrypt password: %w", err)
		}
		db.Password = encrypted
	}

	if err := s.repo.Update(ctx, db); err != nil {
		return nil, fmt.Errorf("failed to update source database: %w", err)
	}
	return db, nil
}

func (s *sourceDatabaseService) DeleteDatabase(ctx context.Context, userID, id uuid.UUID) error {
	if _, err := s.GetDatabase(ctx, userID, id); err != nil {
		return err
	}
	return s.repo.Delete(ctx, id)
}

func (s *sourceDatabaseService) TestConnection(ctx context.Context, userID, id uuid.UUID) domain.ConnectionTestResult {
	db, err := s.GetDatabase(ctx, userID, id)
	if err != nil {
		return domain.ConnectionTestResult{Success: false, Error: err.Error()}
	}

	password, err := s.encryptor.Decrypt(db.Password)
	if err != nil {
		return domain.ConnectionTestResult{Success: false, Error: "failed to decrypt password"}
	}

	dsn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s connect_timeout=5",
		db.Host, db.Port, db.Username, string(password), db.DBName, db.SSLMode)

	start := time.Now()
	conn, err := sql.Open("postgres", dsn)
	if err != nil {
		return domain.ConnectionTestResult{Success: false, Error: err.Error()}
	}
	defer conn.Close()

	pingCtx, cancel := context.WithTimeout(ctx, testConnectionTimeout)
	defer cancel()

	if err := conn.PingContext(pingCtx); err != nil {
		return domain.ConnectionTestResult{Success: false, Error: err.Error()}
	}

	return domain.ConnectionTestResult{Success: true, LatencyMs: time.Since(start).Milliseconds()}
}

func validateInput(in inbound.SourceDatabaseInput, requirePassword bool) error {
	if in.Name == "" || in.Host == "" || in.DBName == "" || in.Username == "" {
		return fmt.Errorf("name, host, dbname and username are required")
	}
	if requirePassword && in.Password == "" {
		return fmt.Errorf("password is required")
	}
	return nil
}

func withDefaultPort(port int) int {
	if port == 0 {
		return 5432
	}
	return port
}

func withDefaultSSLMode(sslmode string) string {
	if sslmode == "" {
		return "disable"
	}
	return sslmode
}
