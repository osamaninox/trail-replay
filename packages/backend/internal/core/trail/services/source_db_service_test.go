package services_test

import (
	"context"
	"testing"

	"github.com/google/uuid"

	"trail-replay/internal/adapters/outbound/storage"
	"trail-replay/internal/core/trail/ports/inbound"
	"trail-replay/internal/core/trail/services"
	"trail-replay/pkg/crypto"
)

func newSourceDBService(t *testing.T) inbound.SourceDatabaseService {
	t.Helper()
	encryptor, err := crypto.NewAESGCMEncryptor("test-encryption-key")
	if err != nil {
		t.Fatalf("encryptor: %v", err)
	}
	return services.NewSourceDatabaseService(storage.NewInMemorySourceDatabaseRepository(), encryptor)
}

func testInput() inbound.SourceDatabaseInput {
	return inbound.SourceDatabaseInput{
		Name:     "prod-db",
		Host:     "db.example.com",
		Port:     5432,
		DBName:   "shop",
		Username: "replicator",
		Password: "s3cret",
		SSLMode:  "require",
	}
}

func TestCreateAndListDatabases(t *testing.T) {
	ctx := context.Background()
	svc := newSourceDBService(t)
	userID := uuid.New()

	created, err := svc.CreateDatabase(ctx, userID, testInput())
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if created.Port != 5432 || created.SSLMode != "require" {
		t.Errorf("unexpected fields: port=%d sslmode=%s", created.Port, created.SSLMode)
	}

	dbs, err := svc.ListDatabases(ctx, userID)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(dbs) != 1 {
		t.Fatalf("expected 1 database, got %d", len(dbs))
	}

	other, err := svc.ListDatabases(ctx, uuid.New())
	if err != nil {
		t.Fatalf("list other: %v", err)
	}
	if len(other) != 0 {
		t.Errorf("expected no databases for other user, got %d", len(other))
	}
}

func TestCreateDatabaseDefaults(t *testing.T) {
	ctx := context.Background()
	svc := newSourceDBService(t)

	in := testInput()
	in.Port = 0
	in.SSLMode = ""

	db, err := svc.CreateDatabase(ctx, uuid.New(), in)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if db.Port != 5432 {
		t.Errorf("expected default port 5432, got %d", db.Port)
	}
	if db.SSLMode != "disable" {
		t.Errorf("expected default sslmode disable, got %s", db.SSLMode)
	}
}

func TestCreateDatabaseRequiresFields(t *testing.T) {
	ctx := context.Background()
	svc := newSourceDBService(t)

	in := testInput()
	in.Password = ""
	if _, err := svc.CreateDatabase(ctx, uuid.New(), in); err == nil {
		t.Error("expected error for missing password")
	}

	in = testInput()
	in.Host = ""
	if _, err := svc.CreateDatabase(ctx, uuid.New(), in); err == nil {
		t.Error("expected error for missing host")
	}
}

func TestUpdateDatabaseKeepsPasswordWhenEmpty(t *testing.T) {
	ctx := context.Background()
	svc := newSourceDBService(t)
	userID := uuid.New()

	created, err := svc.CreateDatabase(ctx, userID, testInput())
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	in := testInput()
	in.Name = "renamed"
	in.Password = ""

	updated, err := svc.UpdateDatabase(ctx, userID, created.ID, in)
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if updated.Name != "renamed" {
		t.Errorf("expected renamed, got %s", updated.Name)
	}
	if string(updated.Password) != string(created.Password) {
		t.Error("expected password to be preserved")
	}
}

func TestDatabaseOwnershipIsolation(t *testing.T) {
	ctx := context.Background()
	svc := newSourceDBService(t)
	owner := uuid.New()
	stranger := uuid.New()

	created, err := svc.CreateDatabase(ctx, owner, testInput())
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	if _, err := svc.GetDatabase(ctx, stranger, created.ID); err == nil {
		t.Error("expected error accessing another user's database")
	}
	if err := svc.DeleteDatabase(ctx, stranger, created.ID); err == nil {
		t.Error("expected error deleting another user's database")
	}
}

func TestTestConnectionFailure(t *testing.T) {
	ctx := context.Background()
	svc := newSourceDBService(t)
	userID := uuid.New()

	in := testInput()
	in.Host = "localhost"
	in.Port = 59999 // unreachable
	created, err := svc.CreateDatabase(ctx, userID, in)
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	result := svc.TestConnection(ctx, userID, created.ID)
	if result.Success {
		t.Error("expected connection to fail")
	}
	if result.Error == "" {
		t.Error("expected an error message")
	}
}
