package main

import (
	"log/slog"
	"net/http"
	"os"

	httphandler "github.com/osamakhalid/trail-replay/internal/adapters/inbound/http"
	"github.com/osamakhalid/trail-replay/internal/adapters/outbound/storage"
	"github.com/osamakhalid/trail-replay/internal/adapters/outbound/storage/postgres"
	"github.com/osamakhalid/trail-replay/internal/core/trail/ports/outbound"
	"github.com/osamakhalid/trail-replay/internal/core/trail/services"
	"github.com/osamakhalid/trail-replay/pkg/config"
	"github.com/osamakhalid/trail-replay/pkg/database"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	cfg := config.Load()

	// Try PostgreSQL first, fallback to in-memory
	var repo outbound.TrailRepository
	db, err := database.NewPostgresConnection(cfg.Database)
	if err != nil {
		slog.Warn("failed to connect to database, using in-memory repository", "error", err)
		repo = storage.NewInMemoryRepository()
	} else {
		slog.Info("connected to postgresql database")
		repo = postgres.NewPostgresRepository(db)
	}

	svc := services.NewTrailService(repo)

	var walRepo outbound.WalQueryRepository
	walDB, err := database.NewPostgresConnection(cfg.WalDatabase)
	if err != nil {
		slog.Warn("failed to connect to WAL database, /wal/transactions will not be available", "error", err)
	} else {
		slog.Info("connected to WAL database")
		walRepo = postgres.NewWalQueryRepository(walDB)
	}
	walSvc := services.NewWalQueryService(walRepo)

	h := httphandler.NewHandler(svc, walSvc)

	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	slog.Info("starting server", "addr", cfg.HTTPAddr)
	if err := http.ListenAndServe(cfg.HTTPAddr, mux); err != nil {
		slog.Error("server failed", "err", err)
		os.Exit(1)
	}
}
