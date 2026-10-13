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
	"fmt"
	"sync"
	"sync/atomic"
	"time"

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

func (t *Txn) heldMode(k Key) (Mode, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	m, ok := t.locks[k]
	return m, ok
}

func (t *Txn) lockKeys() []Key {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]Key, 0, len(t.locks))
	for k := range t.locks {
		out = append(out, k)
	}
	return out
}

func (t *Txn) clearLocks() {
	t.mu.Lock()
	t.locks = nil
	t.mu.Unlock()
}

func (t *Txn) String() string {
	return fmt.Sprintf("txn%d(%s)", t.id, t.State())
}

// Undoer rolls a transaction back. The db layer supplies it, because undo
// needs to touch pages and indexes.
type Undoer interface {
	Rollback(t *Txn) error
}

// Manager creates transactions and ends them.
type Manager struct {
	log    *wal.Manager
	locks  *LockManager
	nextID atomic.Uint64

	mu     sync.Mutex
	active map[ID]*Txn
	undoer Undoer

	commits atomic.Int64
	aborts  atomic.Int64
}

// NewManager creates a transaction manager. firstID is the transaction id to
// start from - on restart it comes from the meta page, so ids never repeat.
func NewManager(log *wal.Manager, lockTimeout time.Duration, firstID ID) *Manager {
	m := &Manager{
		log:    log,
		locks:  NewLockManager(lockTimeout),
		active: make(map[ID]*Txn),
	}
	if firstID < 1 {
		firstID = 1
	}
	m.nextID.Store(firstID)
	return m
}

// SetUndoer installs the rollback implementation.
func (m *Manager) SetUndoer(u Undoer) {
	m.mu.Lock()
	m.undoer = u
	m.mu.Unlock()
}

// Locks exposes the lock manager.
func (m *Manager) Locks() *LockManager { return m.locks }

// NextID is the id the next Begin will hand out (persisted at checkpoint).
func (m *Manager) NextID() ID { return m.nextID.Load() }

// Begin starts a transaction and logs a BEGIN record.
func (m *Manager) Begin() (*Txn, error) { return m.begin(false) }

// BeginReadOnly starts a read-only transaction. It still takes shared locks
// (so it never reads uncommitted data) but writes nothing to the log.
func (m *Manager) BeginReadOnly() (*Txn, error) { return m.begin(true) }

func (m *Manager) begin(readOnly bool) (*Txn, error) {
	t := &Txn{id: m.nextID.Add(1) - 1, readOnly: readOnly, state: StateGrowing}
	rec := &wal.Record{TxnID: t.id, Type: wal.TypeBegin}
	lsn, err := m.log.Append(rec)
	if err != nil {
		return nil, err
	}
	t.SetLastLSN(lsn)
	m.mu.Lock()
	m.active[t.id] = t
	m.mu.Unlock()
	return t, nil
}

// Commit makes the transaction durable and releases its locks.
//
// The order matters and is the whole of Phase 7 in three lines: write the
// COMMIT record, force the log to stable storage, and only then tell the
// caller it worked. The data pages the transaction dirtied may still be sitting
// in the buffer pool - that is fine, because the log can rebuild them.
func (m *Manager) Commit(t *Txn) error {
	if t == nil {
		return ErrUnknownTxn
	}
	if s := t.State(); s == StateCommitted || s == StateAborted {
		return fmt.Errorf("txn: commit of a %s transaction", s)
	}
	if t.Aborting() {
		// Doomed by the deadlock detector: the only legal outcome is a rollback.
		_ = m.Abort(t)
		return ErrDeadlock
	}
	if !t.readOnly {
		lsn, err := m.log.Append(&wal.Record{TxnID: t.id, Type: wal.TypeCommit, PrevLSN: t.LastLSN()})
		if err != nil {
			return err
		}
		t.SetLastLSN(lsn)
		if err := m.log.Flush(lsn + 1); err != nil {
			return fmt.Errorf("txn: commit flush: %w", err)
		}
	}
	t.setState(StateCommitted)
	m.locks.ReleaseAll(t)
	if !t.readOnly {
		// END is bookkeeping only: it tells recovery this transaction needs no
		// further attention. It does not have to be flushed.
		if lsn, err := m.log.Append(&wal.Record{TxnID: t.id, Type: wal.TypeEnd, PrevLSN: t.LastLSN()}); err == nil {
			t.SetLastLSN(lsn)
		}
	}
	m.forget(t)
	m.commits.Add(1)
	return nil
}

// Abort rolls the transaction back: undo its changes (writing compensation
// records as it goes), then release its locks.
func (m *Manager) Abort(t *Txn) error {
	if t == nil {
		return ErrUnknownTxn
	}
	if s := t.State(); s == StateCommitted || s == StateAborted {
		return nil
	}
	m.mu.Lock()
	u := m.undoer
	m.mu.Unlock()

	var undoErr error
	if u != nil && !t.readOnly {
		undoErr = u.Rollback(t)
	}
	if !t.readOnly {
		if lsn, err := m.log.Append(&wal.Record{TxnID: t.id, Type: wal.TypeAbort, PrevLSN: t.LastLSN()}); err == nil {
			t.SetLastLSN(lsn)
		}
		if lsn, err := m.log.Append(&wal.Record{TxnID: t.id, Type: wal.TypeEnd, PrevLSN: t.LastLSN()}); err == nil {
			t.SetLastLSN(lsn)
		}
	}
	t.setState(StateAborted)
	m.locks.ReleaseAll(t)
	m.forget(t)
	m.aborts.Add(1)
	return undoErr
}

func (m *Manager) forget(t *Txn) {
	m.mu.Lock()
	delete(m.active, t.id)
	m.mu.Unlock()
}

// Active returns the ids and undo chain heads of the running transactions.
// The checkpoint writes this out as the "active transaction table".
func (m *Manager) Active() map[ID]wal.LSN {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[ID]wal.LSN, len(m.active))
	for id, t := range m.active {
		out[id] = t.LastLSN()
	}
	return out
}

// ActiveCount is the number of running transactions.
func (m *Manager) ActiveCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.active)
}

// AbortAll rolls back everything still running (used by Close).
func (m *Manager) AbortAll() {
	m.mu.Lock()
	txns := make([]*Txn, 0, len(m.active))
	for _, t := range m.active {
		txns = append(txns, t)
	}
	m.mu.Unlock()
	for _, t := range txns {
		_ = m.Abort(t)
	}
}

// Stats returns commit/abort counters.
func (m *Manager) Stats() (commits, aborts int64, deadlocks int) {
	return m.commits.Load(), m.aborts.Load(), m.locks.Deadlocks()
}
