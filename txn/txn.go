// Package txn implements Phase 9: transactions, and the locking that gives
// them isolation.
//
// A transaction here is the unit of atomicity: every page change it makes is
// tagged with its id and chained through the log by PrevLSN, so the whole
// chain can be walked backwards and undone if it never commits.
//
//	Begin()  -> BEGIN record, id assigned, undo chain starts
//	Commit() -> COMMIT record, log flushed (this is the durability point),
//	            locks released, END record
//	Abort()  -> undo the chain writing CLRs, ABORT/END records, locks released
//
// The package owns transaction state. It deliberately does not know how to
// undo a page change - that needs the buffer pool and the index - so rollback
// is injected by the db layer as an Undoer.
package txn

import (
	"sync"

	"minidb/wal"
)

// ID identifies a transaction. Ids increase over time, which is what lets the
// deadlock detector pick "the youngest" victim.
type ID = uint64

// State is where a transaction is in its life cycle.
type State uint8

const (
	// StateGrowing is the phase in which locks may be acquired (2PL).
	StateGrowing State = iota
	// StateShrinking is the phase after the first release; no new locks.
	StateShrinking
	StateCommitted
	StateAborted
)

func (s State) String() string {
	switch s {
	case StateGrowing:
		return "growing"
	case StateShrinking:
		return "shrinking"
	case StateCommitted:
		return "committed"
	case StateAborted:
		return "aborted"
	}
	return "?"
}

// Txn is a running transaction.
type Txn struct {
	id       ID
	readOnly bool

	mu       sync.Mutex
	state    State
	aborting bool // doomed by the deadlock detector
	lastLSN  wal.LSN
	locks    map[Key]Mode

	// pages touched by this transaction, used by the recovery manager's
	// bookkeeping and by tests.
	writes int
}

func (t *Txn) ID() ID { return t.id }

// LastLSN is the LSN of this transaction's most recent log record: the head of
// its undo chain.
func (t *Txn) LastLSN() wal.LSN {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.lastLSN
}

// SetLastLSN advances the undo chain head. Called for every log record the
// transaction writes.
func (t *Txn) SetLastLSN(l wal.LSN) {
	t.mu.Lock()
	t.lastLSN = l
	t.writes++
	t.mu.Unlock()
}

// State returns the current life cycle state.
func (t *Txn) State() State {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.state
}

func (t *Txn) setState(s State) {
	t.mu.Lock()
	t.state = s
	t.mu.Unlock()
}

// Aborting reports whether the transaction has been doomed (by a deadlock).
// Every operation checks this, so a doomed transaction stops doing work as
// soon as it touches the engine again.
func (t *Txn) Aborting() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.aborting
}

func (t *Txn) markAborting() {
	t.mu.Lock()
	t.aborting = true
	t.mu.Unlock()
}

// ReadOnly reports whether the transaction promised not to write.
func (t *Txn) ReadOnly() bool { return t.readOnly }

// Writes is the number of log records this transaction has produced.
func (t *Txn) Writes() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.writes
}

func (t *Txn) recordLock(k Key, m Mode) {
	t.mu.Lock()
	if t.locks == nil {
		t.locks = make(map[Key]Mode, 8)
	}
	t.locks[k] = m
	t.mu.Unlock()
}
