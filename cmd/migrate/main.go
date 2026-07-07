package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"

	_ "github.com/lib/pq"
	"github.com/osamakhalid/trail-replay/pkg/config"
	"github.com/pressly/goose/v3"
)

func main() {
	wal := flag.Bool("wal", false, "migrate the WAL storage database (trailwal) instead of the main database")
	flag.Parse()

	cfg := config.Load()
	if *wal {
		cfg.Database.Name = "trailwal"
	}

	db, err := sql.Open("postgres", cfg.Database.DSN())
	if err != nil {
		log.Fatalf("failed to connect: %v", err)
	}
	defer db.Close()

	dir, err := os.Getwd()
	if err != nil {
		log.Fatalf("failed to get working directory: %v", err)
	}

	migrationsDir := filepath.Join(dir, "migrations")
	if err := goose.Up(db, migrationsDir); err != nil {
		fmt.Fprintf(os.Stderr, "migration failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("migration applied successfully")
}
