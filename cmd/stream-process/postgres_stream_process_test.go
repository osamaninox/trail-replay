package main

import (
	"errors"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
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
