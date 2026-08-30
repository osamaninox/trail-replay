package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	"trail-replay/internal/adapters/outbound/storage/postgres"
	pgdb "trail-replay/pkg/database"
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

// unrecoverableError marks failures that retrying cannot fix (invalid config,
// dropped/invalidated replication slot, auth or permission errors). Anything
// else — network blips, PG restarts — is treated as transient and retried.
type unrecoverableError struct{ err error }

func (e *unrecoverableError) Error() string { return e.err.Error() }
func (e *unrecoverableError) Unwrap() error { return e.err }

func isUnrecoverable(err error) bool {
	var ue *unrecoverableError
	return errors.As(err, &ue)
}

// isUnrecoverablePgError reports whether a SQLSTATE / message pair describes
// a failure that retrying can never fix.
func isUnrecoverablePgError(code, message string) bool {
	switch code {
	case pgdb.PgCodeInvalidAuthorization,
		pgdb.PgCodeInvalidPassword,
		pgdb.PgCodeInvalidCatalogName,
		pgdb.PgCodeInsufficientPrivilege,
		pgdb.PgCodeUndefinedObject:
		return true
	}
	// PG 15+: starting replication on an invalidated slot (WAL trimmed past
	// the slot's restart LSN) can never succeed again.
	return strings.Contains(message, "replication slot") && strings.Contains(message, "invalidated")
}

// classifyPgError wraps err in unrecoverableError when it carries a
// non-retryable SQLSTATE; otherwise it returns err unchanged (transient).
func classifyPgError(err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && isUnrecoverablePgError(pgErr.Code, pgErr.Message) {
		return &unrecoverableError{err: err}
	}
	var pqErr *pq.Error
	if errors.As(err, &pqErr) && isUnrecoverablePgError(string(pqErr.Code), pqErr.Message) {
		return &unrecoverableError{err: err}
	}
	return err
}

// backoff implements exponential backoff between reconnection attempts:
// it doubles each wait from min up to max. A run that lasted at least
// healthyAfter is considered healthy and resets the backoff via reset().
type backoff struct {
	min, max     time.Duration
	healthyAfter time.Duration
	cur          time.Duration
}

func newBackoff(min, max, healthyAfter time.Duration) *backoff {
	return &backoff{min: min, max: max, healthyAfter: healthyAfter, cur: min}
}

func (b *backoff) next() time.Duration {
	d := b.cur
	b.cur *= 2
	if b.cur > b.max {
		b.cur = b.max
	}
	return d
}

func (b *backoff) reset() { b.cur = b.min }

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
	conn            *pgconn.PgConn
	db              *sqlx.DB
	sourceDB        *sqlx.DB
	persister       walPersister
	slotName        string
	relations       map[uint32]*pglogrepl.RelationMessage
	relationPKs     map[uint32][]string
	currentTxn      *transactionState
	lastLSN         pglogrepl.LSN
	lastStandbySent time.Time
}

func NewPostgresStreamer(ctx context.Context) (*PostgresStreamer, error) {
	// Replication connection for WAL streaming
	connString := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s replication=database",
		dbHost, dbPort, dbUser, dbPassword, dbName)

	config, err := pgconn.ParseConfig(connString)
	if err != nil {
		// Bad connection config can never be fixed by retrying.
		return nil, &unrecoverableError{err: fmt.Errorf("failed to parse replication config: %w", err)}
	}

	conn, err := pgconn.ConnectConfig(ctx, config)
	if err != nil {
		return nil, classifyPgError(fmt.Errorf("failed to connect for replication: %w", err))
	}

	// Regular DB connection for persisting WAL changes (separate DB to avoid recursion)
	dbConnString := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		dbHost, dbPort, dbUser, dbPassword, walDBName)

	db, err := sqlx.ConnectContext(ctx, "postgres", dbConnString)
	if err != nil {
		conn.Close(context.Background())
		return nil, classifyPgError(fmt.Errorf("failed to connect to database for persistence: %w", err))
	}

	// Source DB connection for metadata queries (PK lookup, etc.)
	sourceDBConnString := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		dbHost, dbPort, dbUser, dbPassword, dbName)

	sourceDB, err := sqlx.ConnectContext(ctx, "postgres", sourceDBConnString)
	if err != nil {
		db.Close()
		conn.Close(context.Background())
		return nil, classifyPgError(fmt.Errorf("failed to connect to source database for metadata: %w", err))
	}

	return &PostgresStreamer{
		conn:        conn,
		db:          db,
		sourceDB:    sourceDB,
		persister:   &sqlxWalPersister{db: db},
		slotName:    slotName,
		relations:   make(map[uint32]*pglogrepl.RelationMessage),
		relationPKs: make(map[uint32][]string),
	}, nil
}

func (ps *PostgresStreamer) createReplicationSlot(ctx context.Context) error {
	// First, create a publication for all tables
	pubQuery := "CREATE PUBLICATION trail_replay_pub FOR ALL TABLES"
	result := ps.conn.Exec(ctx, pubQuery)
	_, err := result.ReadAll()
	if err != nil {
		// Check if publication already exists
		if pgErr, ok := err.(*pgconn.PgError); ok && pgErr.Code == "42710" {
			log.Printf("Publication 'trail_replay_pub' already exists, continuing...")
		} else {
			return classifyPgError(fmt.Errorf("failed to create publication: %v", err))
		}
	} else {
		log.Printf("Created publication: trail_replay_pub")
	}

	// Create logical replication slot using pgoutput plugin
	query := fmt.Sprintf("SELECT pg_create_logical_replication_slot('%s', 'pgoutput')", ps.slotName)

	result = ps.conn.Exec(ctx, query)
	_, err = result.ReadAll()
	if err != nil {
		// Check if slot already exists
		if pgErr, ok := err.(*pgconn.PgError); ok && pgErr.Code == "42710" {
			log.Printf("Replication slot '%s' already exists, continuing...", ps.slotName)
			return nil
		}
		return classifyPgError(fmt.Errorf("failed to create replication slot: %v", err))
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
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (source_slot, commit_lsn) DO NOTHING
		RETURNING id`

	if err := tx.QueryRow(insertTxnQuery,
		entity.SourceSlot, entity.SourceDb, entity.Xid,
		entity.BeginLSN, entity.CommitLSN, entity.CommitTS, entity.ChangeCount,
	).Scan(&entity.ID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		return fmt.Errorf("failed to insert wal_transaction: %w", err)
	}

	if len(changes) == 0 {
		return tx.Commit()
	}

	const colsPerRow = 10
	valuePlaceholders := make([]string, 0, len(changes))
	args := make([]interface{}, 0, len(changes)*colsPerRow)

	for i := range changes {
		base := i * colsPerRow
		valuePlaceholders = append(valuePlaceholders, fmt.Sprintf(
			"($%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d)",
			base+1, base+2, base+3, base+4, base+5, base+6, base+7, base+8, base+9, base+10,
		))
		args = append(args,
			entity.ID,
			changes[i].ChangeSeqInTxn,
			changes[i].SchemaName,
			changes[i].TableName,
			changes[i].TableOID,
			changes[i].Op,
			changes[i].OldRow,
			changes[i].NewRow,
			changes[i].ForwardDMLSQL,
			changes[i].ReverseDMLSQL,
		)
	}

	query := fmt.Sprintf(`INSERT INTO wal_change (transaction_id, change_seq_in_txn, schema_name, table_name, table_oid, op, old_row, new_row, forward_dml_sql, reverse_dml_sql) VALUES %s`,
		strings.Join(valuePlaceholders, ","))

	if _, err := tx.Exec(query, args...); err != nil {
		return fmt.Errorf("failed to insert wal_changes: %w", err)
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

func (ps *PostgresStreamer) startReplication(ctx context.Context) error {
	lsn, err := ps.loadCheckpoint()
	if err != nil {
		return fmt.Errorf("failed to load checkpoint: %v", err)
	}
	ps.lastLSN = lsn

	pluginArguments := []string{
		"proto_version '1'",
		"publication_names 'trail_replay_pub'",
	}

	err = pglogrepl.StartReplication(ctx, ps.conn, ps.slotName, lsn, pglogrepl.StartReplicationOptions{
		PluginArgs: pluginArguments,
	})
	if err != nil {
		return classifyPgError(fmt.Errorf("failed to start replication: %v", err))
	}

	log.Printf("Started logical replication on slot: %s at LSN: %s", ps.slotName, lsn.String())
	return nil
}

func (ps *PostgresStreamer) streamChanges(ctx context.Context) error {
	log.Println("Starting to stream changes...")
	log.Println("===== PostgreSQL Logical Replication Stream Output =====")
	log.Println("Waiting for database changes... (make some INSERT/UPDATE/DELETE operations)")

	for {
		// Blocks until a message arrives or ctx is cancelled (shutdown signal).
		msg, err := ps.conn.ReceiveMessage(ctx)

		if err != nil {
			if ctx.Err() != nil {
				return nil // graceful shutdown requested
			}
			return classifyPgError(fmt.Errorf("failed to receive message: %v", err))
		}

		switch msg := msg.(type) {
		case *pgproto3.CopyData:
			ps.processCopyData(msg)
		case *pgproto3.ErrorResponse:
			log.Printf("ERROR: %s", msg.Message)
			pgErr := fmt.Errorf("postgres error (SQLSTATE %s): %s", msg.Code, msg.Message)
			if isUnrecoverablePgError(msg.Code, msg.Message) {
				return &unrecoverableError{err: pgErr}
			}
			if msg.Severity == "FATAL" || msg.Severity == "PANIC" {
				// The server is about to drop the connection; reconnect.
				return pgErr
			}
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
		log.Printf("[KEEPALIVE] Replication slot keepalive message")
		if time.Since(ps.lastStandbySent) > 10*time.Second && ps.lastLSN > 0 {
			log.Printf("[KEEPALIVE] Stream process standby update")
			ps.sendStandbyStatusUpdate(ps.lastLSN)
		}
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

		if _, cached := ps.relationPKs[m.RelationID]; !cached {
			pks := ps.loadRelationPKs(m.Namespace, m.RelationName)
			ps.relationPKs[m.RelationID] = pks
			log.Printf("[RELATION] PK columns for %s.%s: %v", m.Namespace, m.RelationName, pks)
		}

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
			pkCols := ps.getRelationPK(rel)
			newRow := tupleToRow(rel, m.Tuple)
			forwardDML := generateInsertSQL(rel.Namespace, rel.RelationName, newRow)
			reverseDML := generateDeleteSQL(rel.Namespace, rel.RelationName, pkCols, newRow)

			ps.currentTxn.changes = append(ps.currentTxn.changes, postgres.WalChangeEntity{
				ChangeSeqInTxn: int32(len(ps.currentTxn.changes) + 1),
				SchemaName:     rel.Namespace,
				TableName:      rel.RelationName,
				TableOID:       &m.RelationID,
				Op:             "I",
				NewRow:         newRow,
				ChangedColumns: []string{},
				ForwardDMLSQL:  forwardDML,
				ReverseDMLSQL:  reverseDML,
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
			pkCols := ps.getRelationPK(rel)
			changedCols := diffTuples(rel, m.OldTuple, m.NewTuple)
			oldRow := tupleToRow(rel, m.OldTuple)
			newRow := tupleToRow(rel, m.NewTuple)
			reverseDML := generateReverseDML(rel.Namespace, rel.RelationName, pkCols, changedCols, oldRow, newRow)
			forwardDML := generateForwardDML(rel.Namespace, rel.RelationName, pkCols, changedCols, oldRow, newRow)

			ps.currentTxn.changes = append(ps.currentTxn.changes, postgres.WalChangeEntity{
				ChangeSeqInTxn: int32(len(ps.currentTxn.changes) + 1),
				SchemaName:     rel.Namespace,
				TableName:      rel.RelationName,
				TableOID:       &m.RelationID,
				Op:             "U",
				OldRow:         oldRow,
				NewRow:         newRow,
				ChangedColumns: changedCols,
				ReverseDMLSQL:  reverseDML,
				ForwardDMLSQL:  forwardDML,
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
			pkCols := ps.getRelationPK(rel)
			oldRow := tupleToRow(rel, m.OldTuple)
			forwardDML := generateDeleteSQL(rel.Namespace, rel.RelationName, pkCols, oldRow)
			reverseDML := generateInsertSQL(rel.Namespace, rel.RelationName, oldRow)

			ps.currentTxn.changes = append(ps.currentTxn.changes, postgres.WalChangeEntity{
				ChangeSeqInTxn: int32(len(ps.currentTxn.changes) + 1),
				SchemaName:     rel.Namespace,
				TableName:      rel.RelationName,
				TableOID:       &m.RelationID,
				Op:             "D",
				OldRow:         oldRow,
				ChangedColumns: []string{},
				ForwardDMLSQL:  forwardDML,
				ReverseDMLSQL:  reverseDML,
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
	ps.sendStandbyStatusUpdate(commitLSN)
}

func (ps *PostgresStreamer) persistCheckpoint(lsn pglogrepl.LSN) {
	if lsn <= ps.lastLSN {
		return
	}
	if err := ps.saveCheckpoint(lsn); err != nil {
		log.Printf("FAILED to save checkpoint: %v", err)
	}
}

func (ps *PostgresStreamer) sendStandbyStatusUpdate(lsn pglogrepl.LSN) {
	if ps.conn == nil {
		return
	}
	err := pglogrepl.SendStandbyStatusUpdate(context.Background(), ps.conn, pglogrepl.StandbyStatusUpdate{
		WALWritePosition: lsn,
	})
	ps.lastStandbySent = time.Now()
	if err != nil {
		log.Printf("FAILED to send standby status update (LSN=%s): %v", lsn.String(), err)
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

func (ps *PostgresStreamer) loadRelationPKs(schema, table string) []string {
	query := `
		SELECT kcu.column_name
		FROM information_schema.table_constraints tc
		JOIN information_schema.key_column_usage kcu
			ON tc.constraint_name = kcu.constraint_name
			AND tc.table_schema = kcu.table_schema
		WHERE tc.constraint_type = 'PRIMARY KEY'
			AND tc.table_schema = $1
			AND tc.table_name = $2
		ORDER BY kcu.ordinal_position`

	rows, err := ps.sourceDB.Query(query, schema, table)
	if err != nil {
		log.Printf("[WARN] failed to query PK columns for %s.%s: %v", schema, table, err)
		return nil
	}
	defer rows.Close()

	var pks []string
	for rows.Next() {
		var colName string
		if err := rows.Scan(&colName); err != nil {
			log.Printf("[WARN] failed to scan PK column for %s.%s: %v", schema, table, err)
			continue
		}
		pks = append(pks, colName)
	}
	return pks
}

func (ps *PostgresStreamer) getRelationPK(rel *pglogrepl.RelationMessage) []string {
	if pks, ok := ps.relationPKs[rel.RelationID]; ok {
		return pks
	}
	var pks []string
	for _, col := range rel.Columns {
		if col.Flags == 1 {
			pks = append(pks, col.Name)
		}
	}
	ps.relationPKs[rel.RelationID] = pks
	return pks
}

func diffTuples(rel *pglogrepl.RelationMessage, oldT, newT *pglogrepl.TupleData) []string {
	var changed []string
	for i := range newT.Columns {
		if i >= len(rel.Columns) {
			break
		}
		colName := rel.Columns[i].Name

		if oldT == nil || i >= len(oldT.Columns) {
			changed = append(changed, colName)
			continue
		}

		oldCol := oldT.Columns[i]
		newCol := newT.Columns[i]

		if oldCol.DataType != newCol.DataType {
			changed = append(changed, colName)
			continue
		}
		if oldCol.DataType == 'n' && newCol.DataType == 'n' {
			continue
		}
		if oldCol.DataType == 'u' || newCol.DataType == 'u' {
			if oldCol.DataType != newCol.DataType {
				changed = append(changed, colName)
			}
			continue
		}
		if string(oldCol.Data) != string(newCol.Data) {
			changed = append(changed, colName)
		}
	}
	return changed
}

func formatSQLValue(v any) string {
	if v == nil {
		return "NULL"
	}
	s := fmt.Sprintf("%v", v)
	s = strings.ReplaceAll(s, "'", "''")
	return "'" + s + "'"
}

func generateReverseDML(schema, table string, pkCols, changedCols []string, oldRow, newRow *postgres.PayloadJSON) string {
	if oldRow == nil || *oldRow == nil {
		log.Printf("[WARN] No old row data for %s.%s — reverse DML requires REPLICA IDENTITY FULL", schema, table)
		return ""
	}
	o := *oldRow

	setClauses := make([]string, 0, len(changedCols))
	for _, col := range changedCols {
		oldVal, ok := o[col]
		if !ok {
			continue
		}
		setClauses = append(setClauses, fmt.Sprintf("%s = %s", pgQuoteIdent(col), formatSQLValue(oldVal)))
	}
	if len(setClauses) == 0 {
		return ""
	}

	whereClauses := make([]string, 0, len(pkCols))
	rowForWhere := o
	if newRow != nil && *newRow != nil {
		rowForWhere = *newRow
	}
	for _, pk := range pkCols {
		val, ok := rowForWhere[pk]
		if !ok {
			continue
		}
		whereClauses = append(whereClauses, fmt.Sprintf("%s = %s", pgQuoteIdent(pk), formatSQLValue(val)))
	}
	if len(whereClauses) == 0 {
		return ""
	}

	return fmt.Sprintf("UPDATE %s.%s SET %s WHERE %s",
		pgQuoteIdent(schema), pgQuoteIdent(table),
		strings.Join(setClauses, ", "),
		strings.Join(whereClauses, " AND "))
}

func generateForwardDML(schema, table string, pkCols, changedCols []string, oldRow, newRow *postgres.PayloadJSON) string {
	if newRow == nil || *newRow == nil {
		return ""
	}
	n := *newRow

	setClauses := make([]string, 0, len(changedCols))
	for _, col := range changedCols {
		newVal, ok := n[col]
		if !ok {
			continue
		}
		setClauses = append(setClauses, fmt.Sprintf("%s = %s", pgQuoteIdent(col), formatSQLValue(newVal)))
	}
	if len(setClauses) == 0 {
		return ""
	}

	whereClauses := make([]string, 0, len(pkCols))
	for _, pk := range pkCols {
		val, ok := n[pk]
		if !ok {
			continue
		}
		whereClauses = append(whereClauses, fmt.Sprintf("%s = %s", pgQuoteIdent(pk), formatSQLValue(val)))
	}
	if len(whereClauses) == 0 {
		return ""
	}

	return fmt.Sprintf("UPDATE %s.%s SET %s WHERE %s",
		pgQuoteIdent(schema), pgQuoteIdent(table),
		strings.Join(setClauses, ", "),
		strings.Join(whereClauses, " AND "))
}

func pgQuoteIdent(ident string) string {
	ident = strings.ReplaceAll(ident, "\"", "\"\"")
	return "\"" + ident + "\""
}

func generateInsertSQL(schema, table string, row *postgres.PayloadJSON) string {
	if row == nil || *row == nil {
		return ""
	}
	r := *row
	cols := make([]string, 0, len(r))
	vals := make([]string, 0, len(r))
	for col, val := range r {
		cols = append(cols, pgQuoteIdent(col))
		vals = append(vals, formatSQLValue(val))
	}
	return fmt.Sprintf("INSERT INTO %s.%s (%s) VALUES (%s)",
		pgQuoteIdent(schema), pgQuoteIdent(table),
		strings.Join(cols, ", "),
		strings.Join(vals, ", "))
}

func generateDeleteSQL(schema, table string, pkCols []string, row *postgres.PayloadJSON) string {
	if row == nil || *row == nil || len(pkCols) == 0 {
		return ""
	}
	r := *row
	whereClauses := make([]string, 0, len(pkCols))
	for _, pk := range pkCols {
		val, ok := r[pk]
		if !ok {
			continue
		}
		whereClauses = append(whereClauses, fmt.Sprintf("%s = %s", pgQuoteIdent(pk), formatSQLValue(val)))
	}
	if len(whereClauses) == 0 {
		return ""
	}
	return fmt.Sprintf("DELETE FROM %s.%s WHERE %s",
		pgQuoteIdent(schema), pgQuoteIdent(table),
		strings.Join(whereClauses, " AND "))
}

func (ps *PostgresStreamer) cleanup() {
	if ps.db != nil {
		ps.sourceDB.Close()
		ps.db.Close()
	}
	if ps.conn != nil {
		ps.conn.Close(context.Background())
	}
}

// run streams WAL changes, reconnecting with exponential backoff whenever a
// transient failure (network blip, PG restart) kills the stream. It returns
// nil on graceful shutdown (ctx cancelled) and non-nil only for unrecoverable
// errors that retrying cannot fix — the process exits and leaves those to
// external supervision.
func run(ctx context.Context, streamFn func(ctx context.Context) error, backOff *backoff) error {
	for attempt := 1; ; attempt++ {
		start := time.Now()
		err := streamFn(ctx)

		if ctx.Err() != nil {
			return nil
		}
		if err == nil {
			log.Printf("Stream ended unexpectedly; reconnecting")
		} else if isUnrecoverable(err) {
			return err
		} else {
			log.Printf("Stream connection lost (attempt %d): %v", attempt, err)
		}

		// A run that stayed up a while was healthy; the next failure restarts
		// the backoff from the minimum delay.
		if time.Since(start) >= backOff.healthyAfter {
			backOff.reset()
		}
		wait := backOff.next()
		log.Printf("Reconnecting in %s...", wait)
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(wait):
		}
	}
}

// streamOnce performs one full connect → setup → stream cycle. The caller
// decides, based on the classified error, whether to retry or give up.
func streamOnce(ctx context.Context) error {
	streamer, err := NewPostgresStreamer(ctx)
	if err != nil {
		return err
	}
	defer streamer.cleanup()

	// Create replication slot (idempotent — resumes from the persisted checkpoint)
	if err := streamer.createReplicationSlot(ctx); err != nil {
		return err
	}

	// Start replication from the last persisted LSN
	if err := streamer.startReplication(ctx); err != nil {
		return err
	}

	// Stream changes until the connection drops or shutdown is requested
	return streamer.streamChanges(ctx)
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	log.Println("PostgreSQL Logical Replication Stream")
	log.Println("=========================================")

	// SIGINT/SIGTERM cancel the context, which unblocks ReceiveMessage and
	// runs cleanup via streamOnce's defer — no more os.Exit mid-stream.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	backOff := newBackoff(1*time.Second, 60*time.Second, 10*time.Second)
	if err := run(ctx, streamOnce, backOff); err != nil {
		log.Fatalf("Unrecoverable error, exiting: %v", err)
	}
	log.Println("Shutdown complete.")
}
