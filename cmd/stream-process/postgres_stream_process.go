package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jmoiron/sqlx"
	"github.com/osamakhalid/trail-replay/internal/adapters/outbound/storage/postgres"
)

const (
	// Database connection details from your docker-compose
	dbHost     = "localhost"
	dbPort     = "5433"
	dbUser     = "trailuser"
	dbPassword = "trailpass"
	dbName     = "traildb"
	walDBName  = "trailwal"

	// Replication slot name for our POC
	slotName = "trail_replay_poc_slot"
)

type transactionState struct {
	xid       int64
	beginLSN  string
	commitLSN string
	commitTS  time.Time
	changes   []postgres.WalChangeEntity
}

type walPersister interface {
	LoadCheckpoint(slotName string) (pglogrepl.LSN, error)
	SaveCheckpoint(slotName string, lsn pglogrepl.LSN) error
	PersistTransaction(entity *postgres.WalTransactionEntity, changes []postgres.WalChangeEntity) error
}

type sqlxWalPersister struct {
	db *sqlx.DB
}

type PostgresStreamer struct {
	conn       *pgconn.PgConn
	db         *sqlx.DB
	persister  walPersister
	slotName   string
	relations  map[uint32]*pglogrepl.RelationMessage
	currentTxn *transactionState
	lastLSN    pglogrepl.LSN
}

func NewPostgresStreamer() (*PostgresStreamer, error) {
	// Replication connection for WAL streaming
	connString := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s replication=database",
		dbHost, dbPort, dbUser, dbPassword, dbName)

	config, err := pgconn.ParseConfig(connString)
	if err != nil {
		return nil, fmt.Errorf("failed to parse replication config: %w", err)
	}

	conn, err := pgconn.ConnectConfig(context.Background(), config)
	if err != nil {
		return nil, fmt.Errorf("failed to connect for replication: %w", err)
	}

	// Regular DB connection for persisting WAL changes (separate DB to avoid recursion)
	dbConnString := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		dbHost, dbPort, dbUser, dbPassword, walDBName)

	db, err := sqlx.Connect("postgres", dbConnString)
	if err != nil {
		conn.Close(context.Background())
		return nil, fmt.Errorf("failed to connect to database for persistence: %w", err)
	}

	return &PostgresStreamer{
		conn:      conn,
		db:        db,
		persister: &sqlxWalPersister{db: db},
		slotName:  slotName,
		relations: make(map[uint32]*pglogrepl.RelationMessage),
	}, nil
}

func (ps *PostgresStreamer) createReplicationSlot() error {
	// First, create a publication for all tables
	pubQuery := "CREATE PUBLICATION trail_replay_pub FOR ALL TABLES"
	result := ps.conn.Exec(context.Background(), pubQuery)
	_, err := result.ReadAll()
	if err != nil {
		// Check if publication already exists
		if pgErr, ok := err.(*pgconn.PgError); ok && pgErr.Code == "42710" {
			log.Printf("Publication 'trail_replay_pub' already exists, continuing...")
		} else {
			return fmt.Errorf("failed to create publication: %v", err)
		}
	} else {
		log.Printf("Created publication: trail_replay_pub")
	}

	// Create logical replication slot using pgoutput plugin
	query := fmt.Sprintf("SELECT pg_create_logical_replication_slot('%s', 'pgoutput')", ps.slotName)

	result = ps.conn.Exec(context.Background(), query)
	_, err = result.ReadAll()
	if err != nil {
		// Check if slot already exists
		if pgErr, ok := err.(*pgconn.PgError); ok && pgErr.Code == "42710" {
			log.Printf("Replication slot '%s' already exists, continuing...", ps.slotName)
			return nil
		}
		return fmt.Errorf("failed to create replication slot: %v", err)
	}

	log.Printf("Created replication slot: %s", ps.slotName)
	return nil
}

func (p *sqlxWalPersister) LoadCheckpoint(slotName string) (pglogrepl.LSN, error) {
	var lsn string
	err := p.db.Get(&lsn, `SELECT last_lsn::text FROM wal_checkpoint WHERE slot_name = $1`, slotName)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("failed to load checkpoint: %w", err)
	}
	checkpointLSN, err := pglogrepl.ParseLSN(lsn)
	if err != nil {
		return 0, fmt.Errorf("failed to parse checkpoint LSN: %w", err)
	}
	return checkpointLSN, nil
}

func (p *sqlxWalPersister) SaveCheckpoint(slotName string, lsn pglogrepl.LSN) error {
	_, err := p.db.Exec(
		`INSERT INTO wal_checkpoint (slot_name, last_lsn, updated_at) VALUES ($1, $2, now())
		 ON CONFLICT (slot_name) DO UPDATE SET last_lsn = $2, updated_at = now()`,
		slotName, lsn.String(),
	)
	if err != nil {
		return fmt.Errorf("failed to save checkpoint: %w", err)
	}
	return nil
}

func (p *sqlxWalPersister) PersistTransaction(entity *postgres.WalTransactionEntity, changes []postgres.WalChangeEntity) error {
	ctx := context.Background()
	tx, err := p.db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin DB transaction: %w", err)
	}
	defer tx.Rollback()

	insertTxnQuery := `
		INSERT INTO wal_transaction (source_slot, source_db, xid, begin_lsn, commit_lsn, commit_ts, change_count)
		VALUES (:source_slot, :source_db, :xid, :begin_lsn, :commit_lsn, :commit_ts, :change_count)
		ON CONFLICT (source_slot, commit_lsn) DO NOTHING
		RETURNING id`

	rows, err := tx.NamedQuery(insertTxnQuery, entity)
	if err != nil {
		return fmt.Errorf("failed to insert wal_transaction: %w", err)
	}
	defer rows.Close()

	if !rows.Next() {
		return nil
	}
	if err := rows.Scan(&entity.ID); err != nil {
		return fmt.Errorf("failed to scan transaction ID: %w", err)
	}

	insertChangeQuery := `
		INSERT INTO wal_change (transaction_id, change_seq_in_txn, schema_name, table_name, table_oid, op, old_row, new_row, forward_dml_sql, reverse_dml_sql)
		VALUES (:transaction_id, :change_seq_in_txn, :schema_name, :table_name, :table_oid, :op, :old_row, :new_row, :forward_dml_sql, :reverse_dml_sql)`

	for i := range changes {
		changes[i].TransactionID = entity.ID
		if _, err := tx.NamedExec(insertChangeQuery, &changes[i]); err != nil {
			return fmt.Errorf("failed to insert wal_change (seq=%d): %w", changes[i].ChangeSeqInTxn, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit DB transaction: %w", err)
	}

	return nil
}

func (ps *PostgresStreamer) loadCheckpoint() (pglogrepl.LSN, error) {
	lsn, err := ps.persister.LoadCheckpoint(ps.slotName)
	if err != nil {
		return 0, err
	}
	if lsn != 0 {
		log.Printf("Loaded checkpoint LSN for slot %s: %s", ps.slotName, lsn.String())
	}
	return lsn, nil
}

func (ps *PostgresStreamer) saveCheckpoint(lsn pglogrepl.LSN) error {
	if err := ps.persister.SaveCheckpoint(ps.slotName, lsn); err != nil {
		return err
	}
	ps.lastLSN = lsn
	return nil
}

func (ps *PostgresStreamer) dropReplicationSlot() error {
	query := fmt.Sprintf("SELECT pg_drop_replication_slot('%s')", ps.slotName)
	result := ps.conn.Exec(context.Background(), query)
	_, err := result.ReadAll()
	if err != nil {
		log.Printf("Warning: failed to drop replication slot: %v", err)
		return err
	}
	log.Printf("Dropped replication slot: %s", ps.slotName)
	return nil
}

func (ps *PostgresStreamer) startReplication() error {
	lsn, err := ps.loadCheckpoint()
	if err != nil {
		return fmt.Errorf("failed to load checkpoint: %v", err)
	}
	ps.lastLSN = lsn

	pluginArguments := []string{
		"proto_version '1'",
		"publication_names 'trail_replay_pub'",
	}

	err = pglogrepl.StartReplication(context.Background(), ps.conn, ps.slotName, lsn, pglogrepl.StartReplicationOptions{
		PluginArgs: pluginArguments,
	})
	if err != nil {
		return fmt.Errorf("failed to start replication: %v", err)
	}

	log.Printf("Started logical replication on slot: %s at LSN: %s", ps.slotName, lsn.String())
	return nil
}

func (ps *PostgresStreamer) streamChanges() error {
	log.Println("Starting to stream changes...")
	log.Println("===== PostgreSQL Logical Replication Stream Output =====")
	log.Println("Waiting for database changes... (make some INSERT/UPDATE/DELETE operations)")

	for {
		// No timeout - wait indefinitely for messages
		msg, err := ps.conn.ReceiveMessage(context.Background())

		if err != nil {
			return fmt.Errorf("failed to receive message: %v", err)
		}

		switch msg := msg.(type) {
		case *pgproto3.CopyData:
			ps.processCopyData(msg)
		case *pgproto3.ErrorResponse:
			log.Printf("ERROR: %s", msg.Message)
		default:
			log.Printf("Received message type: %T", msg)
		}
	}
}

func (ps *PostgresStreamer) processCopyData(msg *pgproto3.CopyData) {
	data := msg.Data

	if len(data) == 0 {
		return
	}

	// First byte indicates the message type
	msgType := data[0]

	switch msgType {
	case 'w': // XLogData
		ps.processXLogData(data[1:])
	case 'k': // Primary keepalive message
		log.Printf("[KEEPALIVE] Server keepalive message")
	default:
		log.Printf("[UNKNOWN] Message type: %c, Data: %x", msgType, data)
	}
}

func (ps *PostgresStreamer) processXLogData(data []byte) {
	if len(data) < 8 {
		log.Printf("[XLOGDATA] Insufficient data length: %d", len(data))
		return
	}

	// Skip WAL start LSN (8 bytes) and WAL end LSN (8 bytes)
	// and timestamp (8 bytes) = 24 bytes total header
	if len(data) < 24 {
		log.Printf("[XLOGDATA] Insufficient header data length: %d", len(data))
		return
	}

	payload := data[24:]

	if len(payload) == 0 {
		log.Printf("[XLOGDATA] Empty payload")
		return
	}

	// Parse the logical replication message
	ps.parseLogicalMessage(payload)
}

func (ps *PostgresStreamer) parseLogicalMessage(data []byte) {
	if len(data) == 0 {
		return
	}

	timestamp := time.Now().Format("2006-01-02 15:04:05.000")

	msg, err := pglogrepl.Parse(data)
	if err != nil {
		log.Printf("[%s] PARSE ERROR: %v", timestamp, err)
		log.Printf("  Raw data: %x", data)
		return
	}

	switch m := msg.(type) {
	case *pglogrepl.BeginMessage:
		log.Printf("[%s] BEGIN TRANSACTION", timestamp)
		log.Printf("  Transaction ID: %d", m.Xid)
		log.Printf("  LSN: %s", m.FinalLSN)

		ps.currentTxn = &transactionState{
			xid:      int64(m.Xid),
			beginLSN: m.FinalLSN.String(),
		}

	case *pglogrepl.CommitMessage:
		log.Printf("[%s] COMMIT TRANSACTION", timestamp)
		log.Printf("  LSN: %s", m.CommitLSN)

		if ps.currentTxn != nil {
			ps.currentTxn.commitLSN = m.CommitLSN.String()
			ps.currentTxn.commitTS = m.CommitTime
			ps.persistCurrentTxn()
			ps.currentTxn = nil
		}

	case *pglogrepl.RelationMessage:
		log.Printf("[%s] RELATION/TABLE SCHEMA", timestamp)
		log.Printf("  Relation ID: %d", m.RelationID)
		log.Printf("  Namespace: %s", m.Namespace)
		log.Printf("  Table: %s", m.RelationName)
		log.Printf("  Columns: %d", len(m.Columns))
		for i, col := range m.Columns {
			log.Printf("    Column %d: %s (Type: %d)", i, col.Name, col.DataType)
		}
		ps.relations[m.RelationID] = m

	case *pglogrepl.InsertMessage:
		log.Printf("[%s] INSERT OPERATION", timestamp)
		log.Printf("  Relation ID: %d", m.RelationID)
		rel, exists := ps.relations[m.RelationID]
		if !exists {
			log.Printf("  Table: Unknown (relation %d)", m.RelationID)
			return
		}
		log.Printf("  Table: %s.%s", rel.Namespace, rel.RelationName)
		ps.printTupleData("  New Row", rel, m.Tuple)

		if ps.currentTxn != nil {
			ps.currentTxn.changes = append(ps.currentTxn.changes, postgres.WalChangeEntity{
				ChangeSeqInTxn: int32(len(ps.currentTxn.changes) + 1),
				SchemaName:     rel.Namespace,
				TableName:      rel.RelationName,
				TableOID:       &m.RelationID,
				Op:             "I",
				NewRow:         tupleToRow(rel, m.Tuple),
			})
		}

	case *pglogrepl.UpdateMessage:
		log.Printf("[%s] UPDATE OPERATION", timestamp)
		log.Printf("  Relation ID: %d", m.RelationID)
		rel, exists := ps.relations[m.RelationID]
		if !exists {
			log.Printf("  Table: Unknown (relation %d)", m.RelationID)
			return
		}
		log.Printf("  Table: %s.%s", rel.Namespace, rel.RelationName)
		if m.OldTuple != nil {
			ps.printTupleData("  Old Row", rel, m.OldTuple)
		}
		ps.printTupleData("  New Row", rel, m.NewTuple)

		if ps.currentTxn != nil {
			ps.currentTxn.changes = append(ps.currentTxn.changes, postgres.WalChangeEntity{
				ChangeSeqInTxn: int32(len(ps.currentTxn.changes) + 1),
				SchemaName:     rel.Namespace,
				TableName:      rel.RelationName,
				TableOID:       &m.RelationID,
				Op:             "U",
				OldRow:         tupleToRow(rel, m.OldTuple),
				NewRow:         tupleToRow(rel, m.NewTuple),
			})
		}

	case *pglogrepl.DeleteMessage:
		log.Printf("[%s] DELETE OPERATION", timestamp)
		log.Printf("  Relation ID: %d", m.RelationID)
		rel, exists := ps.relations[m.RelationID]
		if !exists {
			log.Printf("  Table: Unknown (relation %d)", m.RelationID)
			return
		}
		log.Printf("  Table: %s.%s", rel.Namespace, rel.RelationName)
		if m.OldTuple != nil {
			ps.printTupleData("  Deleted Row", rel, m.OldTuple)
		}

		if ps.currentTxn != nil {
			ps.currentTxn.changes = append(ps.currentTxn.changes, postgres.WalChangeEntity{
				ChangeSeqInTxn: int32(len(ps.currentTxn.changes) + 1),
				SchemaName:     rel.Namespace,
				TableName:      rel.RelationName,
				TableOID:       &m.RelationID,
				Op:             "D",
				OldRow:         tupleToRow(rel, m.OldTuple),
			})
		}

	case *pglogrepl.TruncateMessage:
		log.Printf("[%s] TRUNCATE OPERATION", timestamp)
		log.Printf("  Relations: %v", m.RelationIDs)

	default:
		log.Printf("[%s] UNKNOWN MESSAGE TYPE: %T", timestamp, msg)
	}

	log.Println("  ---")
}

func (ps *PostgresStreamer) persistCurrentTxn() {
	txn := ps.currentTxn
	if txn == nil || len(txn.changes) == 0 {
		return
	}

	txnEntity := &postgres.WalTransactionEntity{
		SourceSlot:  ps.slotName,
		SourceDb:    dbName,
		Xid:         txn.xid,
		CommitLSN:   txn.commitLSN,
		CommitTS:    txn.commitTS,
		ChangeCount: int32(len(txn.changes)),
	}
	if txn.beginLSN != "" {
		txnEntity.BeginLSN = &txn.beginLSN
	}

	commitLSN, parseErr := pglogrepl.ParseLSN(txn.commitLSN)
	if parseErr != nil {
		log.Printf("FAILED to parse commit LSN: %v", parseErr)
		return
	}

	if err := ps.persister.PersistTransaction(txnEntity, txn.changes); err != nil {
		log.Printf("FAILED to persist WAL transaction (xid=%d): %v", txn.xid, err)
		return
	}
	log.Printf("[PERSISTED] WAL transaction %d with %d changes", txn.xid, len(txn.changes))

	ps.persistCheckpoint(commitLSN)
}

func (ps *PostgresStreamer) persistCheckpoint(lsn pglogrepl.LSN) {
	if lsn <= ps.lastLSN {
		return
	}
	if err := ps.saveCheckpoint(lsn); err != nil {
		log.Printf("FAILED to save checkpoint: %v", err)
	}
}

func tupleToRow(rel *pglogrepl.RelationMessage, tuple *pglogrepl.TupleData) *postgres.PayloadJSON {
	if tuple == nil {
		return nil
	}
	row := make(postgres.PayloadJSON)
	for i, col := range tuple.Columns {
		if i >= len(rel.Columns) {
			break
		}
		switch col.DataType {
		case 'n':
			row[rel.Columns[i].Name] = nil
		case 'u':
			row[rel.Columns[i].Name] = "<unchanged>"
		case 't':
			row[rel.Columns[i].Name] = string(col.Data)
		}
	}
	return &row
}

func (ps *PostgresStreamer) printTupleData(prefix string, rel *pglogrepl.RelationMessage, tuple *pglogrepl.TupleData) {
	if tuple == nil {
		log.Printf("%s: <nil>", prefix)
		return
	}

	log.Printf("%s:", prefix)
	for i, col := range tuple.Columns {
		if i >= len(rel.Columns) {
			break
		}
		colName := rel.Columns[i].Name

		switch col.DataType {
		case 'n': // null
			log.Printf("    %s: NULL", colName)
		case 'u': // unchanged (for UPDATE old tuple)
			log.Printf("    %s: <unchanged>", colName)
		case 't': // text data
			log.Printf("    %s: %q", colName, string(col.Data))
		default:
			log.Printf("    %s: %q (type: %c)", colName, string(col.Data), col.DataType)
		}
	}
}

func (ps *PostgresStreamer) cleanup() {
	if ps.db != nil {
		ps.db.Close()
	}
	if ps.conn != nil {
		ps.dropReplicationSlot()
		ps.conn.Close(context.Background())
	}
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	log.Println("PostgreSQL Logical Replication Stream POC")
	log.Println("=========================================")

	streamer, err := NewPostgresStreamer()
	if err != nil {
		log.Fatalf("Failed to create streamer: %v", err)
	}

	// Setup graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Println("\nReceived shutdown signal, cleaning up...")
		streamer.cleanup()
		os.Exit(0)
	}()

	// Create replication slot
	if err := streamer.createReplicationSlot(); err != nil {
		log.Fatalf("Failed to create replication slot: %v", err)
	}

	// Start replication
	if err := streamer.startReplication(); err != nil {
		log.Fatalf("Failed to start replication: %v", err)
	}

	// Stream changes
	if err := streamer.streamChanges(); err != nil {
		log.Fatalf("Failed to stream changes: %v", err)
	}
}
