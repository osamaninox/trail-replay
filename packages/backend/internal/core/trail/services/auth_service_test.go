package services_test

import (
	"context"
	"testing"
	"time"

	"trail-replay/internal/adapters/outbound/storage"
	"trail-replay/internal/core/trail/ports/inbound"
	"trail-replay/internal/core/trail/services"
)

func newAuthService() inbound.AuthService {
	return services.NewAuthService(storage.NewInMemoryUserRepository(), "test-secret", time.Hour)
}

func TestSignupAndLogin(t *testing.T) {
	ctx := context.Background()
	svc := newAuthService()

	result, err := svc.Signup(ctx, "user@example.com", "secret-password")
	if err != nil {
		t.Fatalf("signup: %v", err)
	}
	if result.Token == "" {
		t.Fatal("expected a token")
	}
	if result.User.Email != "user@example.com" {
		t.Errorf("email mismatch: got %q", result.User.Email)
	}

	loggedIn, err := svc.Login(ctx, "user@example.com", "secret-password")
	if err != nil {
		t.Fatalf("login: %v", err)
	}
	if loggedIn.User.ID != result.User.ID {
		t.Errorf("expected same user id on login")
	}
}

func TestSignupRejectsDuplicateEmail(t *testing.T) {
	ctx := context.Background()
	svc := newAuthService()

	if _, err := svc.Signup(ctx, "dup@example.com", "password"); err != nil {
		t.Fatalf("first signup: %v", err)
	}
	if _, err := svc.Signup(ctx, "dup@example.com", "password"); err == nil {
		t.Fatal("expected duplicate email error")
	}
}

func TestLoginRejectsWrongPassword(t *testing.T) {
	ctx := context.Background()
	svc := newAuthService()

	if _, err := svc.Signup(ctx, "user@example.com", "right-password"); err != nil {
		t.Fatalf("signup: %v", err)
	}
	if _, err := svc.Login(ctx, "user@example.com", "wrong-password"); err == nil {
		t.Fatal("expected invalid credentials error")
	}
}

func TestValidateToken(t *testing.T) {
	ctx := context.Background()
	svc := newAuthService()

	result, err := svc.Signup(ctx, "user@example.com", "secret-password")
	if err != nil {
		t.Fatalf("signup: %v", err)
	}

	userID, err := svc.ValidateToken(result.Token)
	if err != nil {
		t.Fatalf("validate: %v", err)
	}
	if userID != result.User.ID {
		t.Errorf("token subject mismatch: got %s want %s", userID, result.User.ID)
	}

	if _, err := svc.ValidateToken("garbage"); err == nil {
		t.Error("expected error for malformed token")
	}

	other := services.NewAuthService(storage.NewInMemoryUserRepository(), "other-secret", time.Hour)
	if _, err := other.ValidateToken(result.Token); err == nil {
		t.Error("expected error for token signed with a different secret")
	}
}
