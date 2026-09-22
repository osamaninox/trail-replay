package domain

import (
	"time"

	"github.com/google/uuid"
)

type SourceDatabase struct {
	ID        uuid.UUID
	UserID    uuid.UUID
	Name      string
	Host      string
	Port      int
	DBName    string
	Username  string
	Password  []byte // AES-GCM encrypted
	SSLMode   string
	CreatedAt time.Time
	UpdatedAt time.Time
}

type SourceDatabaseResponse struct {
	ID        uuid.UUID `json:"id"`
	Name      string    `json:"name"`
	Host      string    `json:"host"`
	Port      int       `json:"port"`
	DBName    string    `json:"dbname"`
	Username  string    `json:"username"`
	SSLMode   string    `json:"sslmode"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

func (d *SourceDatabase) ToResponse() SourceDatabaseResponse {
	return SourceDatabaseResponse{
		ID:        d.ID,
		Name:      d.Name,
		Host:      d.Host,
		Port:      d.Port,
		DBName:    d.DBName,
		Username:  d.Username,
		SSLMode:   d.SSLMode,
		CreatedAt: d.CreatedAt,
		UpdatedAt: d.UpdatedAt,
	}
}

type ConnectionTestResult struct {
	Success   bool   `json:"success"`
	LatencyMs int64  `json:"latency_ms"`
	Error     string `json:"error,omitempty"`
}
