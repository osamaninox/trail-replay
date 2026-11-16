package config

import (
	"fmt"
	"os"
)

type Config struct {
	HTTPAddr      string
	Database      DatabaseConfig
	WalDatabase   DatabaseConfig
	// DBOSDatabaseURL is the DBOS system-database connection string. DBOS
	// keeps its own schema inside the WAL database (plan decision #17), so it
	// defaults to the WAL DSN; DBOS_SYSTEM_DATABASE_URL overrides.
	DBOSDatabaseURL string
	JWTSecret       string
	EncryptionKey   string
	CORSOrigin      string
}

type DatabaseConfig struct {
	Host     string
	Port     string
	User     string
	Password string
	Name     string
	SSLMode  string
}

func (d DatabaseConfig) DSN() string {
	return fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
		d.Host, d.Port, d.User, d.Password, d.Name, d.SSLMode)
}

func Load() Config {
	addr := os.Getenv("HTTP_ADDR")
	if addr == "" {
		addr = ":8080"
	}

	walDBName := os.Getenv("WAL_DB_NAME")
	if walDBName == "" {
		walDBName = "trailwal"
	}

	dbHost := getEnvOrDefault("DB_HOST", "localhost")
	dbPort := getEnvOrDefault("DB_PORT", "5433")
	dbUser := getEnvOrDefault("DB_USER", "trailuser")
	dbPassword := getEnvOrDefault("DB_PASSWORD", "trailpass")
	dbSSLMode := getEnvOrDefault("DB_SSLMODE", "disable")

	walDatabase := DatabaseConfig{
		Host:     dbHost,
		Port:     dbPort,
		User:     dbUser,
		Password: dbPassword,
		Name:     walDBName,
		SSLMode:  dbSSLMode,
	}

	dbosURL := os.Getenv("DBOS_SYSTEM_DATABASE_URL")
	if dbosURL == "" {
		dbosURL = walDatabase.DSN()
	}

	return Config{
		HTTPAddr:        addr,
		JWTSecret:       getEnvOrDefault("JWT_SECRET", "trail-replay-dev-secret"),
		EncryptionKey:   getEnvOrDefault("DB_ENCRYPTION_KEY", "trail-replay-dev-encryption-key"),
		CORSOrigin:      getEnvOrDefault("CORS_ORIGIN", "http://localhost:5173"),
		DBOSDatabaseURL: dbosURL,
		Database: DatabaseConfig{
			Host:     dbHost,
			Port:     dbPort,
			User:     dbUser,
			Password: dbPassword,
			Name:     getEnvOrDefault("DB_NAME", "traildb"),
			SSLMode:  dbSSLMode,
		},
		WalDatabase: walDatabase,
	}
}

func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
