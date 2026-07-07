package config

import (
	"fmt"
	"os"
)

type Config struct {
	HTTPAddr string
	Database DatabaseConfig
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

	return Config{
		HTTPAddr: addr,
		Database: DatabaseConfig{
			Host:     getEnvOrDefault("DB_HOST", "localhost"),
			Port:     getEnvOrDefault("DB_PORT", "5433"),
			User:     getEnvOrDefault("DB_USER", "trailuser"),
			Password: getEnvOrDefault("DB_PASSWORD", "trailpass"),
			Name:     getEnvOrDefault("DB_NAME", "traildb"),
			SSLMode:  getEnvOrDefault("DB_SSLMODE", "disable"),
		},
	}
}

func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
