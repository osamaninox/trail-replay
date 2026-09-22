package domain

import (
	"time"

	"github.com/google/uuid"
)

type User struct {
	ID           uuid.UUID
	Email        string
	PasswordHash string
	CreatedAt    time.Time
}

type UserResponse struct {
	ID    uuid.UUID `json:"id"`
	Email string    `json:"email"`
}

func (u *User) ToResponse() UserResponse {
	return UserResponse{
		ID:    u.ID,
		Email: u.Email,
	}
}

type AuthResult struct {
	Token string       `json:"token"`
	User  UserResponse `json:"user"`
}
