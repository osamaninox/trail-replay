package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"

	httphandler "trail-replay/internal/adapters/inbound/http"
	"trail-replay/internal/adapters/outbound/storage"
	"trail-replay/internal/adapters/outbound/storage/postgres"
	"trail-replay/internal/core/trail/ports/outbound"
	"trail-replay/internal/core/trail/services"
	"trail-replay/pkg/config"
	"trail-replay/pkg/database"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	cfg := config.Load()

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

	var revertHandler *httphandler.RevertHandler

	if walDB != nil {
		revertRepo := postgres.NewRevertRepository(walDB)
		jobCreated := make(chan struct{}, 1)
		revertSvc := services.NewRevertService(revertRepo, jobCreated)
		revertHandler = httphandler.NewRevertHandler(revertSvc)

		if db != nil {
			sourceExecutor := postgres.NewSourceDBExecutor(db)
			services.StartRevertJobRunner(context.Background(), revertRepo, sourceExecutor, jobCreated)
			slog.Info("revert job runner started")
		} else {
			slog.Warn("source database not available, revert job runner will not be started")
		}
	}

	h := httphandler.NewHandler(svc, walSvc)

	mux := http.NewServeMux()
	h.RegisterRoutes(mux)
	if revertHandler != nil {
		revertHandler.RegisterRoutes(mux)
	}

	slog.Info("starting server", "addr", cfg.HTTPAddr)
	if err := http.ListenAndServe(cfg.HTTPAddr, mux); err != nil {
		slog.Error("server failed", "err", err)
		os.Exit(1)
	}
}
