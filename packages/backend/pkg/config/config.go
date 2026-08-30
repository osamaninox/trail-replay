package config

import (
	"fmt"
	"os"
)

type Config struct {
	HTTPAddr    string
	Database    DatabaseConfig
	WalDatabase DatabaseConfig
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

	return Config{
		HTTPAddr: addr,
		Database: DatabaseConfig{
			Host:     dbHost,
			Port:     dbPort,
			User:     dbUser,
			Password: dbPassword,
			Name:     getEnvOrDefault("DB_NAME", "traildb"),
			SSLMode:  dbSSLMode,
		},
		WalDatabase: DatabaseConfig{
			Host:     dbHost,
			Port:     dbPort,
			User:     dbUser,
			Password: dbPassword,
			Name:     walDBName,
			SSLMode:  dbSSLMode,
		},
	}
}

func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
