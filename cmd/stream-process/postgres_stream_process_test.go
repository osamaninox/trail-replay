package main

import (
	"errors"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/osamakhalid/trail-replay/internal/adapters/outbound/storage/postgres"
)

type mockPersister struct {
	loadFn              func(slotName string) (pglogrepl.LSN, error)
	saveFn              func(slotName string, lsn pglogrepl.LSN) error
	persistFn           func(entity *postgres.WalTransactionEntity, changes []postgres.WalChangeEntity) error
	loadCalls           int
	saveCalls           []pglogrepl.LSN
	persistCalls        int
	lastPersistedEntity *postgres.WalTransactionEntity
}

func (m *mockPersister) LoadCheckpoint(slotName string) (pglogrepl.LSN, error) {
	m.loadCalls++
	if m.loadFn != nil {
		return m.loadFn(slotName)
	}
	return 0, nil
}

func (m *mockPersister) SaveCheckpoint(slotName string, lsn pglogrepl.LSN) error {
	m.saveCalls = append(m.saveCalls, lsn)
	if m.saveFn != nil {
		return m.saveFn(slotName, lsn)
	}
	return nil
}

func (m *mockPersister) PersistTransaction(entity *postgres.WalTransactionEntity, changes []postgres.WalChangeEntity) error {
	m.persistCalls++
	m.lastPersistedEntity = entity
	if m.persistFn != nil {
		return m.persistFn(entity, changes)
	}
	entity.ID = uint64(m.persistCalls)
	return nil
}

func newTestStreamer(m *mockPersister) *PostgresStreamer {
	return &PostgresStreamer{
		persister: m,
		slotName:  "test_slot",
	}
}

func TestLoadCheckpoint_NoCheckpoint_ReturnsZero(t *testing.T) {
	m := &mockPersister{
		loadFn: func(slotName string) (pglogrepl.LSN, error) {
			return 0, nil
		},
	}
	ps := newTestStreamer(m)

	lsn, err := ps.loadCheckpoint()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if lsn != 0 {
		t.Errorf("expected LSN 0, got %s", lsn.String())
	}
	if ps.lastLSN != 0 {
		t.Errorf("expected lastLSN 0, got %s", ps.lastLSN.String())
	}
}

func TestLoadCheckpoint_ReturnsStoredLSN(t *testing.T) {
	stored := pglogrepl.LSN(0x16B3748)
	m := &mockPersister{
		loadFn: func(slotName string) (pglogrepl.LSN, error) {
			return stored, nil
		},
	}
	ps := newTestStreamer(m)

	lsn, err := ps.loadCheckpoint()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if lsn != stored {
		t.Errorf("expected %s, got %s", stored.String(), lsn.String())
	}
}

func TestLoadCheckpoint_PropagatesError(t *testing.T) {
	m := &mockPersister{
		loadFn: func(slotName string) (pglogrepl.LSN, error) {
			return 0, errors.New("db connection lost")
		},
	}
	ps := newTestStreamer(m)

	_, err := ps.loadCheckpoint()
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestSaveCheckpoint_UpdatesLastLSN(t *testing.T) {
	m := &mockPersister{}
	ps := newTestStreamer(m)

	lsn := pglogrepl.LSN(0x3000000)
	if err := ps.saveCheckpoint(lsn); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ps.lastLSN != lsn {
		t.Errorf("expected lastLSN %s, got %s", lsn.String(), ps.lastLSN.String())
	}
	if len(m.saveCalls) != 1 || m.saveCalls[0] != lsn {
		t.Errorf("expected saveCall with %s, got %v", lsn.String(), m.saveCalls)
	}
}

func TestSaveCheckpoint_PropagatesError(t *testing.T) {
	m := &mockPersister{
		saveFn: func(slotName string, lsn pglogrepl.LSN) error {
			return errors.New("write failed")
		},
	}
	ps := newTestStreamer(m)

	err := ps.saveCheckpoint(0x5000)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestPersistCheckpoint_NoRegression(t *testing.T) {
	m := &mockPersister{}
	ps := newTestStreamer(m)

	high := pglogrepl.LSN(0x3000000)
	if err := ps.saveCheckpoint(high); err != nil {
		t.Fatalf("saveCheckpoint failed: %v", err)
	}

	low := pglogrepl.LSN(0x2000000)
	ps.persistCheckpoint(low)
	if ps.lastLSN != high {
		t.Errorf("expected lastLSN %s after lower LSN, got %s", high.String(), ps.lastLSN.String())
	}
	if len(m.saveCalls) != 1 {
		t.Errorf("expected no additional save calls, got %d", len(m.saveCalls))
	}

	ps.persistCheckpoint(high)
	if ps.lastLSN != high {
		t.Errorf("expected lastLSN unchanged at %s, got %s", high.String(), ps.lastLSN.String())
	}
	if len(m.saveCalls) != 1 {
		t.Errorf("expected no additional save calls for equal LSN, got %d", len(m.saveCalls))
	}

	higher := pglogrepl.LSN(0x4000000)
	ps.persistCheckpoint(higher)
	if ps.lastLSN != higher {
		t.Errorf("expected lastLSN %s, got %s", higher.String(), ps.lastLSN.String())
	}
	if len(m.saveCalls) != 2 {
		t.Errorf("expected 2 save calls after higher LSN, got %d", len(m.saveCalls))
	}
}

func TestPersistCurrentTxn_PersistsAndCheckpoints(t *testing.T) {
	m := &mockPersister{}
	ps := newTestStreamer(m)

	commitLSNStr := "0/5000"
	commitLSN, _ := pglogrepl.ParseLSN(commitLSNStr)
	ps.currentTxn = &transactionState{
		xid:       100,
		beginLSN:  "0/4500",
		commitLSN: commitLSNStr,
		commitTS:  time.Now(),
		changes: []postgres.WalChangeEntity{
			{ChangeSeqInTxn: 1, SchemaName: "public", TableName: "items", Op: "I", ForwardDMLSQL: "sql1", ReverseDMLSQL: "sql1"},
		},
	}

	ps.persistCurrentTxn()

	if m.persistCalls != 1 {
		t.Errorf("expected 1 persist call, got %d", m.persistCalls)
	}
	if m.lastPersistedEntity.Xid != 100 {
		t.Errorf("expected xid 100, got %d", m.lastPersistedEntity.Xid)
	}
	if ps.lastLSN != commitLSN {
		t.Errorf("expected lastLSN %s, got %s", commitLSN.String(), ps.lastLSN.String())
	}
}

func TestPersistCurrentTxn_EmptyChanges_Skips(t *testing.T) {
	m := &mockPersister{}
	ps := newTestStreamer(m)

	ps.currentTxn = &transactionState{
		xid:       200,
		commitLSN: "0/6000",
		commitTS:  time.Now(),
	}

	ps.persistCurrentTxn()

	if m.persistCalls != 0 {
		t.Errorf("expected no persist calls, got %d", m.persistCalls)
	}
	if len(m.saveCalls) != 0 {
		t.Errorf("expected no save calls, got %d", len(m.saveCalls))
	}
}

func TestPersistCurrentTxn_PersistFailure_DoesNotCheckpoint(t *testing.T) {
	m := &mockPersister{
		persistFn: func(entity *postgres.WalTransactionEntity, changes []postgres.WalChangeEntity) error {
			return errors.New("DB write failed")
		},
	}
	ps := newTestStreamer(m)

	ps.currentTxn = &transactionState{
		xid:       300,
		commitLSN: "0/7000",
		commitTS:  time.Now(),
		changes: []postgres.WalChangeEntity{
			{ChangeSeqInTxn: 1, SchemaName: "public", TableName: "items", Op: "I", ForwardDMLSQL: "sql", ReverseDMLSQL: "sql"},
		},
	}

	ps.persistCurrentTxn()

	if ps.lastLSN != 0 {
		t.Errorf("expected lastLSN 0 after persist failure, got %s", ps.lastLSN.String())
	}
	if len(m.saveCalls) != 0 {
		t.Errorf("expected no save calls after persist failure, got %d", len(m.saveCalls))
	}
}

func TestPersistCheckpoint_CallsSaveCheckpoint(t *testing.T) {
	m := &mockPersister{}
	ps := newTestStreamer(m)

	lsn := pglogrepl.LSN(0x8000000)
	ps.persistCheckpoint(lsn)

	if ps.lastLSN != lsn {
		t.Errorf("expected lastLSN %s, got %s", lsn.String(), ps.lastLSN.String())
	}
	if len(m.saveCalls) != 1 {
		t.Errorf("expected 1 saveCall, got %d", len(m.saveCalls))
	}
	if m.saveCalls[0] != lsn {
		t.Errorf("expected %s, got %s", lsn.String(), m.saveCalls[0].String())
	}
}

func TestPersistCheckpoint_SaveFailure_DoesNotUpdateLastLSN(t *testing.T) {
	m := &mockPersister{
		saveFn: func(slotName string, lsn pglogrepl.LSN) error {
			return errors.New("save failed")
		},
	}
	ps := newTestStreamer(m)

	lsn := pglogrepl.LSN(0x9000000)
	ps.persistCheckpoint(lsn)

	if ps.lastLSN != 0 {
		t.Errorf("expected lastLSN 0 after save failure, got %s", ps.lastLSN.String())
	}
}

// --- formatSQLValue ---

func TestFormatSQLValue_Nil(t *testing.T) {
	got := formatSQLValue(nil)
	if got != "NULL" {
		t.Errorf("expected 'NULL', got %q", got)
	}
}

func TestFormatSQLValue_String(t *testing.T) {
	got := formatSQLValue("hello")
	if got != "'hello'" {
		t.Errorf("expected 'hello', got %q", got)
	}
}

func TestFormatSQLValue_Int(t *testing.T) {
	got := formatSQLValue(42)
	if got != "'42'" {
		t.Errorf("expected '42', got %q", got)
	}
}

func TestFormatSQLValue_SingleQuote(t *testing.T) {
	got := formatSQLValue("it's")
	if got != "'it''s'" {
		t.Errorf("expected \"'it''s'\", got %q", got)
	}
}

func TestFormatSQLValue_Float(t *testing.T) {
	got := formatSQLValue(3.14)
	if got != "'3.14'" {
		t.Errorf("expected '3.14', got %q", got)
	}
}

func TestFormatSQLValue_Bool(t *testing.T) {
	got := formatSQLValue(true)
	if got != "'true'" {
		t.Errorf("expected 'true', got %q", got)
	}
}

// --- pgQuoteIdent ---

func TestPgQuoteIdent_Normal(t *testing.T) {
	got := pgQuoteIdent("trails")
	if got != `"trails"` {
		t.Errorf("expected \"trails\", got %q", got)
	}
}

func TestPgQuoteIdent_WithQuote(t *testing.T) {
	got := pgQuoteIdent(`my"table`)
	if got != `"my""table"` {
		t.Errorf("expected \"my\"\"table\", got %q", got)
	}
}

func TestPgQuoteIdent_Empty(t *testing.T) {
	got := pgQuoteIdent("")
	if got != `""` {
		t.Errorf("expected \"\", got %q", got)
	}
}

// --- generateInsertSQL ---

func TestGenerateInsertSQL_NilRow(t *testing.T) {
	got := generateInsertSQL("public", "trails", nil)
	if got != "" {
		t.Errorf("expected empty, got %q", got)
	}
}

func TestGenerateInsertSQL_EmptyRow(t *testing.T) {
	row := postgres.PayloadJSON{}
	got := generateInsertSQL("public", "trails", &row)
	// Empty map is non-nil so function produces INSERT INTO ... () VALUES ()
	if got != `INSERT INTO "public"."trails" () VALUES ()` {
		t.Errorf("expected empty INSERT, got %q", got)
	}
}

func TestGenerateInsertSQL_SingleColumn(t *testing.T) {
	row := postgres.PayloadJSON{"id": "t1", "name": "test"}
	got := generateInsertSQL("public", "trails", &row)
	// Order is non-deterministic for maps; test both possibilities
	validA := `INSERT INTO "public"."trails" ("id", "name") VALUES ('t1', 'test')`
	validB := `INSERT INTO "public"."trails" ("name", "id") VALUES ('test', 't1')`
	if got != validA && got != validB {
		t.Errorf("got %q", got)
	}
}

func TestGenerateInsertSQL_NullsAndSpecialChars(t *testing.T) {
	row := postgres.PayloadJSON{"id": "t-x", "name": nil, "desc": "it's \"great\""}
	got := generateInsertSQL("public", "trails", &row)
	if !containsAll(got, `'t-x'`, `NULL`, `'it''s "great"'`) {
		t.Errorf("unexpected SQL: %q", got)
	}
	if len(got) < 40 {
		t.Errorf("SQL too short: %q", got)
	}
}

// --- generateDeleteSQL ---

func TestGenerateDeleteSQL_NilRow(t *testing.T) {
	got := generateDeleteSQL("public", "trails", []string{"id"}, nil)
	if got != "" {
		t.Errorf("expected empty, got %q", got)
	}
}

func TestGenerateDeleteSQL_EmptyPKs(t *testing.T) {
	row := postgres.PayloadJSON{"id": "t1"}
	got := generateDeleteSQL("public", "trails", []string{}, &row)
	if got != "" {
		t.Errorf("expected empty, got %q", got)
	}
}

func TestGenerateDeleteSQL_SinglePK(t *testing.T) {
	row := postgres.PayloadJSON{"id": "t1", "name": "test"}
	got := generateDeleteSQL("public", "trails", []string{"id"}, &row)
	expected := `DELETE FROM "public"."trails" WHERE "id" = 't1'`
	if got != expected {
		t.Errorf("got %q", got)
	}
}

func TestGenerateDeleteSQL_MissingPKInRow(t *testing.T) {
	row := postgres.PayloadJSON{"name": "test"}
	got := generateDeleteSQL("public", "trails", []string{"id"}, &row)
	if got != "" {
		t.Errorf("expected empty, got %q", got)
	}
}

func TestGenerateDeleteSQL_MultiPK(t *testing.T) {
	row := postgres.PayloadJSON{"trail_id": "t1", "sequence": 1}
	got := generateDeleteSQL("public", "events", []string{"trail_id", "sequence"}, &row)
	if !containsAll(got, `"trail_id" = 't1'`, `"sequence" = '1'`) {
		t.Errorf("unexpected SQL: %q", got)
	}
}

// --- generateReverseDML ---

func TestGenerateReverseDML_NilOldRow(t *testing.T) {
	got := generateReverseDML("public", "trails", []string{"id"}, []string{"name"}, nil, nil)
	if got != "" {
		t.Errorf("expected empty, got %q", got)
	}
}

func TestGenerateReverseDML_NoChangedCols(t *testing.T) {
	oldRow := postgres.PayloadJSON{"id": "t1", "name": "old"}
	got := generateReverseDML("public", "trails", []string{"id"}, []string{}, &oldRow, nil)
	if got != "" {
		t.Errorf("expected empty, got %q", got)
	}
}

func TestGenerateReverseDML_UpdateRevert(t *testing.T) {
	oldRow := postgres.PayloadJSON{"id": "t1", "name": "old", "score": 100}
	newRow := postgres.PayloadJSON{"id": "t1", "name": "new", "score": 200}
	got := generateReverseDML("public", "trails", []string{"id"}, []string{"name", "score"}, &oldRow, &newRow)
	if !containsAll(got, `SET`, `"name" = 'old'`, `"score" = '100'`, `WHERE`, `"id" = 't1'`) {
		t.Errorf("unexpected SQL: %q", got)
	}
}

func TestGenerateReverseDML_UsesNewRowPK(t *testing.T) {
	oldRow := postgres.PayloadJSON{"id": "old-id", "name": "old-val"}
	newRow := postgres.PayloadJSON{"id": "t1", "name": "new-val"}
	got := generateReverseDML("public", "trails", []string{"id"}, []string{"name"}, &oldRow, &newRow)
	// WHERE clause should use newRow's PK
	if !containsAll(got, `"id" = 't1'`) {
		t.Errorf("expected WHERE to use new row PK 't1', got %q", got)
	}
}

// --- generateForwardDML ---

func TestGenerateForwardDML_NilNewRow(t *testing.T) {
	got := generateForwardDML("public", "trails", []string{"id"}, []string{"name"}, nil, nil)
	if got != "" {
		t.Errorf("expected empty, got %q", got)
	}
}

func TestGenerateForwardDML_NoChangedCols(t *testing.T) {
	newRow := postgres.PayloadJSON{"id": "t1", "name": "new"}
	got := generateForwardDML("public", "trails", []string{"id"}, []string{}, nil, &newRow)
	if got != "" {
		t.Errorf("expected empty, got %q", got)
	}
}

func TestGenerateForwardDML_UpdateApply(t *testing.T) {
	oldRow := postgres.PayloadJSON{"id": "t1", "name": "old"}
	newRow := postgres.PayloadJSON{"id": "t1", "name": "new"}
	got := generateForwardDML("public", "trails", []string{"id"}, []string{"name"}, &oldRow, &newRow)
	if !containsAll(got, `SET`, `"name" = 'new'`, `WHERE`, `"id" = 't1'`) {
		t.Errorf("unexpected SQL: %q", got)
	}
}

func TestGenerateForwardDML_MissingChangedCol(t *testing.T) {
	newRow := postgres.PayloadJSON{"id": "t1", "name": "new"}
	got := generateForwardDML("public", "trails", []string{"id"}, []string{"score"}, nil, &newRow)
	if got != "" {
		t.Errorf("expected empty when changed col missing, got %q", got)
	}
}

// --- tupleToRow ---

func TestTupleToRow_NilTuple(t *testing.T) {
	rel := &pglogrepl.RelationMessage{
		Columns: []*pglogrepl.RelationMessageColumn{
			{Name: "id", DataType: 23},
		},
	}
	row := tupleToRow(rel, nil)
	if row != nil {
		t.Errorf("expected nil, got %v", row)
	}
}

func TestTupleToRow_TextValues(t *testing.T) {
	rel := &pglogrepl.RelationMessage{
		Columns: []*pglogrepl.RelationMessageColumn{
			{Name: "id", DataType: 25},
			{Name: "name", DataType: 25},
		},
	}
	tuple := &pglogrepl.TupleData{
		Columns: []*pglogrepl.TupleDataColumn{
			{DataType: 't', Data: []byte("t1")},
			{DataType: 't', Data: []byte("test")},
		},
	}
	row := tupleToRow(rel, tuple)
	if row == nil {
		t.Fatal("expected non-nil row")
	}
	if (*row)["id"] != "t1" {
		t.Errorf("expected id='t1', got %v", (*row)["id"])
	}
	if (*row)["name"] != "test" {
		t.Errorf("expected name='test', got %v", (*row)["name"])
	}
}

func TestTupleToRow_NullValue(t *testing.T) {
	rel := &pglogrepl.RelationMessage{
		Columns: []*pglogrepl.RelationMessageColumn{
			{Name: "id", DataType: 25},
			{Name: "name", DataType: 25},
		},
	}
	tuple := &pglogrepl.TupleData{
		Columns: []*pglogrepl.TupleDataColumn{
			{DataType: 't', Data: []byte("t1")},
			{DataType: 'n'},
		},
	}
	row := tupleToRow(rel, tuple)
	if row == nil {
		t.Fatal("expected non-nil row")
	}
	if (*row)["id"] != "t1" {
		t.Errorf("expected id='t1', got %v", (*row)["id"])
	}
	if (*row)["name"] != nil {
		t.Errorf("expected name=nil, got %v", (*row)["name"])
	}
}

func TestTupleToRow_UnchangedValue(t *testing.T) {
	rel := &pglogrepl.RelationMessage{
		Columns: []*pglogrepl.RelationMessageColumn{
			{Name: "id", DataType: 25},
			{Name: "name", DataType: 25},
		},
	}
	tuple := &pglogrepl.TupleData{
		Columns: []*pglogrepl.TupleDataColumn{
			{DataType: 't', Data: []byte("t1")},
			{DataType: 'u'},
		},
	}
	row := tupleToRow(rel, tuple)
	if row == nil {
		t.Fatal("expected non-nil row")
	}
	if (*row)["name"] != "<unchanged>" {
		t.Errorf("expected name='<unchanged>', got %v", (*row)["name"])
	}
}

func TestTupleToRow_ExtraColumns(t *testing.T) {
	rel := &pglogrepl.RelationMessage{
		Columns: []*pglogrepl.RelationMessageColumn{
			{Name: "id", DataType: 25},
		},
	}
	tuple := &pglogrepl.TupleData{
		Columns: []*pglogrepl.TupleDataColumn{
			{DataType: 't', Data: []byte("t1")},
			{DataType: 't', Data: []byte("extra")},
		},
	}
	row := tupleToRow(rel, tuple)
	if row == nil {
		t.Fatal("expected non-nil row")
	}
	if len(*row) != 1 {
		t.Errorf("expected 1 column, got %d", len(*row))
	}
	if _, exists := (*row)["name"]; exists {
		t.Error("expected 'name' to be dropped (past relation columns)")
	}
}

// --- diffTuples ---

func TestDiffTuples_SameData(t *testing.T) {
	rel := &pglogrepl.RelationMessage{
		Columns: []*pglogrepl.RelationMessageColumn{
			{Name: "id", DataType: 25},
			{Name: "name", DataType: 25},
		},
	}
	oldT := &pglogrepl.TupleData{
		Columns: []*pglogrepl.TupleDataColumn{
			{DataType: 't', Data: []byte("t1")},
			{DataType: 't', Data: []byte("same")},
		},
	}
	newT := &pglogrepl.TupleData{
		Columns: []*pglogrepl.TupleDataColumn{
			{DataType: 't', Data: []byte("t1")},
			{DataType: 't', Data: []byte("same")},
		},
	}
	changed := diffTuples(rel, oldT, newT)
	if len(changed) != 0 {
		t.Errorf("expected no changes, got %v", changed)
	}
}

func TestDiffTuples_DifferentData(t *testing.T) {
	rel := &pglogrepl.RelationMessage{
		Columns: []*pglogrepl.RelationMessageColumn{
			{Name: "id", DataType: 25},
			{Name: "name", DataType: 25},
			{Name: "score", DataType: 23},
		},
	}
	oldT := &pglogrepl.TupleData{
		Columns: []*pglogrepl.TupleDataColumn{
			{DataType: 't', Data: []byte("t1")},
			{DataType: 't', Data: []byte("old")},
			{DataType: 't', Data: []byte("100")},
		},
	}
	newT := &pglogrepl.TupleData{
		Columns: []*pglogrepl.TupleDataColumn{
			{DataType: 't', Data: []byte("t1")},
			{DataType: 't', Data: []byte("new")},
			{DataType: 'n'},
		},
	}
	changed := diffTuples(rel, oldT, newT)
	if len(changed) != 2 {
		t.Errorf("expected 2 changes (name, score), got %v", changed)
	}
}

func TestDiffTuples_NilOldTuple(t *testing.T) {
	rel := &pglogrepl.RelationMessage{
		Columns: []*pglogrepl.RelationMessageColumn{
			{Name: "id", DataType: 25},
		},
	}
	newT := &pglogrepl.TupleData{
		Columns: []*pglogrepl.TupleDataColumn{
			{DataType: 't', Data: []byte("t1")},
		},
	}
	changed := diffTuples(rel, nil, newT)
	if len(changed) != 1 || changed[0] != "id" {
		t.Errorf("expected ['id'], got %v", changed)
	}
}

func TestDiffTuples_OldToNull(t *testing.T) {
	rel := &pglogrepl.RelationMessage{
		Columns: []*pglogrepl.RelationMessageColumn{
			{Name: "name", DataType: 25},
		},
	}
	oldT := &pglogrepl.TupleData{
		Columns: []*pglogrepl.TupleDataColumn{
			{DataType: 't', Data: []byte("value")},
		},
	}
	newT := &pglogrepl.TupleData{
		Columns: []*pglogrepl.TupleDataColumn{
			{DataType: 'n'},
		},
	}
	changed := diffTuples(rel, oldT, newT)
	if len(changed) != 1 || changed[0] != "name" {
		t.Errorf("expected ['name'], got %v", changed)
	}
}

func TestDiffTuples_BothNull(t *testing.T) {
	rel := &pglogrepl.RelationMessage{
		Columns: []*pglogrepl.RelationMessageColumn{
			{Name: "name", DataType: 25},
		},
	}
	oldT := &pglogrepl.TupleData{
		Columns: []*pglogrepl.TupleDataColumn{
			{DataType: 'n'},
		},
	}
	newT := &pglogrepl.TupleData{
		Columns: []*pglogrepl.TupleDataColumn{
			{DataType: 'n'},
		},
	}
	changed := diffTuples(rel, oldT, newT)
	if len(changed) != 0 {
		t.Errorf("expected no changes for both null, got %v", changed)
	}
}

// --- getRelationPK ---

func TestGetRelationPK_FromCache(t *testing.T) {
	relationID := uint32(16384)
	expected := []string{"id", "tenant_id"}
	ps := &PostgresStreamer{
		relationPKs: map[uint32][]string{relationID: expected},
	}

	rel := &pglogrepl.RelationMessage{RelationID: relationID}
	got := ps.getRelationPK(rel)
	if len(got) != 2 || got[0] != "id" || got[1] != "tenant_id" {
		t.Errorf("expected %v, got %v", expected, got)
	}
}

func TestGetRelationPK_FromFlags(t *testing.T) {
	relationID := uint32(16384)
	ps := &PostgresStreamer{
		relationPKs: make(map[uint32][]string),
	}

	rel := &pglogrepl.RelationMessage{
		RelationID: relationID,
		Columns: []*pglogrepl.RelationMessageColumn{
			{Name: "id", Flags: 1},      // PK flag
			{Name: "name", Flags: 0},    // not PK
			{Name: "tenant_id", Flags: 0},
		},
	}
	got := ps.getRelationPK(rel)
	if len(got) != 1 || got[0] != "id" {
		t.Errorf("expected ['id'], got %v", got)
	}
	// Verify it got cached
	if _, ok := ps.relationPKs[relationID]; !ok {
		t.Error("expected relation PKs to be cached")
	}
}

func TestGetRelationPK_NoPKFlags(t *testing.T) {
	relationID := uint32(16384)
	ps := &PostgresStreamer{
		relationPKs: make(map[uint32][]string),
	}

	rel := &pglogrepl.RelationMessage{
		RelationID: relationID,
		Columns: []*pglogrepl.RelationMessageColumn{
			{Name: "id", Flags: 0},
			{Name: "name", Flags: 0},
		},
	}
	got := ps.getRelationPK(rel)
	if len(got) != 0 {
		t.Errorf("expected empty, got %v", got)
	}
}

// --- processCopyData ---

func TestProcessCopyData_Empty(t *testing.T) {
	ps := &PostgresStreamer{}
	// Should not panic
	ps.processCopyData(&pgproto3.CopyData{Data: []byte{}})
}

func TestProcessCopyData_Keepalive(t *testing.T) {
	ps := &PostgresStreamer{}
	// Should not panic; keepalive is a no-op when lastStandbySent is zero
	ps.processCopyData(&pgproto3.CopyData{Data: []byte{'k'}})
}

func TestProcessCopyData_UnknownType(t *testing.T) {
	ps := &PostgresStreamer{}
	// Should not panic
	ps.processCopyData(&pgproto3.CopyData{Data: []byte{'x', 0x01, 0x02}})
}

func TestProcessCopyData_XLogData(t *testing.T) {
	ps := &PostgresStreamer{
		relations:   make(map[uint32]*pglogrepl.RelationMessage),
		relationPKs: make(map[uint32][]string),
	}
	// Construct minimal XLogData: 'w' marker + 24 header bytes + empty payload
	data := make([]byte, 25)
	data[0] = 'w'
	// 24 bytes of header (WAL start 8, WAL end 8, timestamp 8)
	// payload (byte 24 onwards) is empty → processXLogData logs and returns
	ps.processCopyData(&pgproto3.CopyData{Data: data})
}

// --- processXLogData ---

func TestProcessXLogData_LessThan8(t *testing.T) {
	ps := &PostgresStreamer{}
	// Should not panic
	ps.processXLogData([]byte{0x00, 0x01})
}

func TestProcessXLogData_LessThan24(t *testing.T) {
	ps := &PostgresStreamer{}
	// Should not panic
	ps.processXLogData(make([]byte, 16))
}

func TestProcessXLogData_Exactly24EmptyPayload(t *testing.T) {
	ps := &PostgresStreamer{}
	// Should not panic
	ps.processXLogData(make([]byte, 24))
}

// --- parseLogicalMessage ---

func TestParseLogicalMessage_EmptyData(t *testing.T) {
	ps := &PostgresStreamer{}
	// Should not panic
	ps.parseLogicalMessage([]byte{})
}

func TestParseLogicalMessage_InvalidWAL(t *testing.T) {
	ps := &PostgresStreamer{}
	// Should not panic on invalid data
	ps.parseLogicalMessage([]byte{0xFF, 0xEE, 0xDD})
}

// --- persistCurrentTxn edge cases ---

func TestPersistCurrentTxn_NilTxn(t *testing.T) {
	m := &mockPersister{}
	ps := newTestStreamer(m)
	ps.currentTxn = nil
	// Should not panic
	ps.persistCurrentTxn()
	if m.persistCalls != 0 {
		t.Errorf("expected no persist, got %d", m.persistCalls)
	}
}

func TestPersistCurrentTxn_InvalidCommitLSN(t *testing.T) {
	m := &mockPersister{}
	ps := newTestStreamer(m)
	ps.currentTxn = &transactionState{
		xid:       400,
		commitLSN: "not-a-valid-lsn",
		commitTS:  time.Now(),
		changes: []postgres.WalChangeEntity{
			{ChangeSeqInTxn: 1, SchemaName: "public", TableName: "items", Op: "I", ForwardDMLSQL: "sql", ReverseDMLSQL: "sql"},
		},
	}
	// Should not panic, should not persist
	ps.persistCurrentTxn()
	if m.persistCalls != 0 {
		t.Errorf("expected no persist for invalid LSN, got %d", m.persistCalls)
	}
}

// --- helper ---

func containsAll(sql string, parts ...string) bool {
	for _, p := range parts {
		if !stringContains(sql, p) {
			return false
		}
	}
	return true
}

func stringContains(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
