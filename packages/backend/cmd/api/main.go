package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos"
	"github.com/jackc/pgx/v5/pgxpool"

	httphandler "trail-replay/internal/adapters/inbound/http"
	"trail-replay/internal/adapters/inbound/workflow"
	"trail-replay/internal/adapters/outbound/storage"
	"trail-replay/internal/adapters/outbound/storage/postgres"
	"trail-replay/internal/core/trail/ports/outbound"
	"trail-replay/internal/core/trail/services"
	"trail-replay/pkg/config"
	"trail-replay/pkg/crypto"
	"trail-replay/pkg/database"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	cfg := config.Load()

	db, err := database.NewPostgresConnection(cfg.Database)
	if err != nil {
		slog.Warn("failed to connect to database, user/database endpoints will use in-memory storage", "error", err)
	} else {
		slog.Info("connected to postgresql database")
	}

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

	if walDB != nil && db != nil {
		revertRepo := postgres.NewRevertRepository(walDB)
		sourceExecutor := postgres.NewSourceDBExecutor(db)

		dbosCtx, err := dbos.NewContext(context.Background(), dbos.Config{
			AppName:     "trail-replay-api",
			DatabaseURL: cfg.DBOSDatabaseURL,
		})
		if err != nil {
			slog.Error("failed to init DBOS context, revert endpoints disabled", "error", err)
		} else {
			// DBOS resolves *sql.DB data sources to its SQLite dialect
			// (datasource.go) — the WAL data source must be a pgx pool.
			walPool, err := pgxpool.New(context.Background(), cfg.WalDatabase.DSN())
			if err != nil {
				slog.Error("failed to create WAL pgx pool, revert endpoints disabled", "error", err)
			} else {
				defer walPool.Close()
				submitter, err := startRevertEngine(dbosCtx, revertRepo, sourceExecutor, walPool)
				if err != nil {
					slog.Error("failed to start revert engine, revert endpoints disabled", "error", err)
				} else {
					revertSvc := services.NewRevertService(revertRepo, submitter, sourceExecutor)
					revertHandler = httphandler.NewRevertHandler(revertSvc)
					defer dbos.Shutdown(dbosCtx, 10*time.Second)
					slog.Info("revert engine started")
				}
			}
		}
	} else if walDB != nil {
		slog.Warn("source database not available, revert endpoints disabled")
	}

	h := httphandler.NewHandler(walSvc)

	encryptor, err := crypto.NewAESGCMEncryptor(cfg.EncryptionKey)
	if err != nil {
		slog.Error("failed to initialize encryptor", "error", err)
		os.Exit(1)
	}

	var userRepo outbound.UserRepository
	var sourceDBRepo outbound.SourceDatabaseRepository
	if db != nil {
		userRepo = postgres.NewUserRepository(db)
		sourceDBRepo = postgres.NewSourceDatabaseRepository(db)
	} else {
		userRepo = storage.NewInMemoryUserRepository()
		sourceDBRepo = storage.NewInMemorySourceDatabaseRepository()
	}

	authSvc := services.NewAuthService(userRepo, cfg.JWTSecret, 24*time.Hour)
	sourceDBSvc := services.NewSourceDatabaseService(sourceDBRepo, encryptor)
	authHandler := httphandler.NewAuthHandler(authSvc)
	databaseHandler := httphandler.NewDatabaseHandler(sourceDBSvc)

	mux := http.NewServeMux()
	h.RegisterRoutes(mux)
	if revertHandler != nil {
		revertHandler.RegisterRoutes(mux)
	}
	authHandler.RegisterRoutes(mux)
	databaseHandler.RegisterRoutes(mux)

	var handler http.Handler = mux
	handler = httphandler.AuthMiddleware(authSvc, handler)
	handler = httphandler.CORSMiddleware(cfg.CORSOrigin, handler)

	slog.Info("starting server", "addr", cfg.HTTPAddr)
	if err := http.ListenAndServe(cfg.HTTPAddr, handler); err != nil {
		slog.Error("server failed", "err", err)
		os.Exit(1)
	}
}

// startRevertEngine registers the WAL database as a DBOS data source, the
// revert queue and workflow, then launches the runtime (which creates/migrates
// the dbos schema and recovers interrupted workflows) and resubmits pending
// jobs — closing the create-vs-enqueue window (plan §4). Resubmission is
// idempotent: DBOS dedups by workflow ID (= job ID).
func startRevertEngine(dbosCtx dbos.Context, revertRepo outbound.RevertRepository, executor outbound.SourceDBExecutor, walPool *pgxpool.Pool) (outbound.RevertJobSubmitter, error) {
	walDS, err := dbos.NewDataSource(dbosCtx, walPool, dbos.WithDataSourceName("wal"))
	if err != nil {
		return nil, fmt.Errorf("register WAL data source: %w", err)
	}

	submitter, err := workflow.NewRevertWorkflow(dbosCtx, revertRepo, executor, walDS)
	if err != nil {
		return nil, err
	}

	if err := dbos.Launch(dbosCtx); err != nil {
		return nil, fmt.Errorf("launch DBOS: %w", err)
	}

	pending, err := revertRepo.ListPendingJobIDs(context.Background())
	if err != nil {
		slog.Error("startup reconciliation scan failed", "error", err)
		return submitter, nil
	}
	for _, id := range pending {
		if err := submitter.Submit(context.Background(), id); err != nil {
			slog.Error("startup reconciliation resubmit failed", "job", id, "error", err)
		}
	}
	if len(pending) > 0 {
		slog.Info("startup reconciliation resubmitted pending jobs", "count", len(pending))
	}
	return submitter, nil
}
