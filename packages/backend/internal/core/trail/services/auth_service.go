package services

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"golang.org/x/crypto/bcrypt"

	"trail-replay/internal/core/trail/domain"
	"trail-replay/internal/core/trail/ports/inbound"
	"trail-replay/internal/core/trail/ports/outbound"
)

var (
	ErrEmailTaken         = errors.New("email already registered")
	ErrInvalidCredentials = errors.New("invalid email or password")
)

type authService struct {
	users     outbound.UserRepository
	secret    []byte
	tokenTTL  time.Duration
	now       func() time.Time
}

func NewAuthService(users outbound.UserRepository, jwtSecret string, tokenTTL time.Duration) inbound.AuthService {
	return &authService{
		users:    users,
		secret:   []byte(jwtSecret),
		tokenTTL: tokenTTL,
		now:      time.Now,
	}
}

func (s *authService) Signup(ctx context.Context, email, password string) (*domain.AuthResult, error) {
	email = strings.TrimSpace(strings.ToLower(email))
	if email == "" || password == "" {
		return nil, fmt.Errorf("email and password are required")
	}

	if _, err := s.users.FindByEmail(ctx, email); err == nil {
		return nil, ErrEmailTaken
	}

	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return nil, fmt.Errorf("failed to hash password: %w", err)
	}

	user := &domain.User{
		ID:           uuid.New(),
		Email:        email,
		PasswordHash: string(hash),
		CreatedAt:    s.now(),
	}
	if err := s.users.Save(ctx, user); err != nil {
		return nil, fmt.Errorf("failed to save user: %w", err)
	}

	return s.issueToken(user)
}

func (s *authService) Login(ctx context.Context, email, password string) (*domain.AuthResult, error) {
	email = strings.TrimSpace(strings.ToLower(email))
	user, err := s.users.FindByEmail(ctx, email)
	if err != nil {
		return nil, ErrInvalidCredentials
	}

	if err := bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte(password)); err != nil {
		return nil, ErrInvalidCredentials
	}

	return s.issueToken(user)
}

func (s *authService) issueToken(user *domain.User) (*domain.AuthResult, error) {
	now := s.now()
	claims := jwt.RegisteredClaims{
		Subject:   user.ID.String(),
		IssuedAt:  jwt.NewNumericDate(now),
		ExpiresAt: jwt.NewNumericDate(now.Add(s.tokenTTL)),
	}
	token, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString(s.secret)
	if err != nil {
		return nil, fmt.Errorf("failed to sign token: %w", err)
	}
	return &domain.AuthResult{Token: token, User: user.ToResponse()}, nil
}

func (s *authService) ValidateToken(tokenStr string) (uuid.UUID, error) {
	token, err := jwt.Parse(tokenStr, func(t *jwt.Token) (any, error) {
		if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", t.Header["alg"])
		}
		return s.secret, nil
	})
	if err != nil {
		return uuid.Nil, fmt.Errorf("invalid token: %w", err)
	}

	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok || !token.Valid {
		return uuid.Nil, fmt.Errorf("invalid token claims")
	}

	sub, err := claims.GetSubject()
	if err != nil {
		return uuid.Nil, fmt.Errorf("missing subject: %w", err)
	}
	userID, err := uuid.Parse(sub)
	if err != nil {
		return uuid.Nil, fmt.Errorf("invalid subject: %w", err)
	}
	return userID, nil
}
