package txn

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"minidb/storage"
)

// ---------------------------------------------------------------------------
// The lock manager: isolation, and what happens when two transactions want the
// same row.
//
// Protocol: strict two-phase locking. A transaction acquires locks as it goes
// and holds every one of them until it commits or aborts. "Strict" (releasing
// only at end of transaction rather than at the start of the shrinking phase)
// is not optional here: undo works by restoring before images, so nobody else
// may be allowed to see - or overwrite - an uncommitted value before the
// rollback has had its chance.
//
// Deadlock is handled by detection rather than prevention: when a transaction
// is about to block, we add its edges to the wait-for graph and look for a
// cycle. If there is one, the youngest transaction in it is aborted.
// ---------------------------------------------------------------------------

// Errors from the lock manager.
var (
	ErrDeadlock     = errors.New("txn: deadlock detected, transaction aborted")
	ErrLockTimeout  = errors.New("txn: timed out waiting for a lock")
	ErrTxnAborted   = errors.New("txn: transaction has been aborted")
	ErrNotGrowing   = errors.New("txn: cannot acquire locks after the shrinking phase has begun")
	ErrUpgradeRace  = errors.New("txn: conflicting lock upgrade")
	ErrUnknownTxn   = errors.New("txn: unknown transaction")
	ErrNotSupported = errors.New("txn: unsupported lock request")
)

// Mode is a lock mode.
type Mode uint8

const (
	// LockShared allows concurrent readers.
	LockShared Mode = iota
	// LockExclusive allows exactly one holder and blocks readers.
	LockExclusive
)

func (m Mode) String() string {
	if m == LockShared {
		return "S"
	}
	return "X"
}

// compatible reports whether two modes can be held at the same time by
// different transactions.
func compatible(a, b Mode) bool { return a == LockShared && b == LockShared }

// Kind distinguishes the objects that can be locked.
type Kind uint8

const (
	KindRow Kind = iota
	KindTable
)

// Key identifies a lockable object. Row locks give per-record isolation;
// table locks are used for schema changes and full-table operations.
type Key struct {
	Kind  Kind
	Table uint32
	RID   storage.RID
}

// RowKey is the lock key for one record of one table.
func RowKey(table uint32, rid storage.RID) Key {
	return Key{Kind: KindRow, Table: table, RID: rid}
}

// TableKey is the lock key for a whole table.
func TableKey(table uint32) Key { return Key{Kind: KindTable, Table: table} }

func (k Key) String() string {
	if k.Kind == KindTable {
		return fmt.Sprintf("table:%d", k.Table)
	}
	return fmt.Sprintf("row:%d:%v", k.Table, k.RID)
}

type waiter struct {
	txn  *Txn
	mode Mode
	ch   chan error
}

type lockEntry struct {
	key     Key
	holders map[ID]Mode
	waiters []*waiter
}

// LockManager arbitrates access to rows and tables.
type LockManager struct {
	mu      sync.Mutex
	table   map[Key]*lockEntry
	timeout time.Duration
	// waitingOn records, for each waiting transaction, the key it is blocked
	// on. That plus the holder sets is the wait-for graph.
	waitingOn map[ID]Key
	txns      map[ID]*Txn

	deadlocks int
}

// NewLockManager creates a lock manager with the given lock wait timeout.
func NewLockManager(timeout time.Duration) *LockManager {
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	return &LockManager{
		table:     make(map[Key]*lockEntry),
		waitingOn: make(map[ID]Key),
		txns:      make(map[ID]*Txn),
		timeout:   timeout,
	}
}

// Deadlocks returns how many deadlocks have been broken.
func (lm *LockManager) Deadlocks() int {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	return lm.deadlocks
}

// Acquire takes a lock for t, blocking until it is granted, the wait times
// out, or a deadlock forces t to abort.
func (lm *LockManager) Acquire(t *Txn, key Key, mode Mode) error {
	if t == nil {
		return nil // system operations bypass locking
	}
	lm.mu.Lock()
	lm.txns[t.ID()] = t

	if t.Aborting() {
		lm.mu.Unlock()
		return ErrTxnAborted
	}
	// Already hold something on this key?
	if held, ok := t.heldMode(key); ok {
		if held == LockExclusive || mode == LockShared {
			lm.mu.Unlock()
			return nil // already strong enough
		}
		// S -> X upgrade.
		e := lm.table[key]
		for id := range e.holders {
			if id != t.ID() {
				// Somebody else holds S. Wait for them the same way a fresh
				// request would, but keep our S lock (that is what makes
				// upgrades deadlock-prone, hence the detection below).
				return lm.waitFor(t, key, LockExclusive, e)
			}
		}
		e.holders[t.ID()] = LockExclusive
		t.recordLock(key, LockExclusive)
		lm.mu.Unlock()
		return nil
	}

	e := lm.table[key]
	if e == nil {
		e = &lockEntry{key: key, holders: make(map[ID]Mode)}
		lm.table[key] = e
	}
	if lm.grantable(e, t.ID(), mode) {
		e.holders[t.ID()] = mode
		t.recordLock(key, mode)
		lm.mu.Unlock()
		return nil
	}
	return lm.waitFor(t, key, mode, e)
}

// grantable reports whether a request can be satisfied immediately. Waiters
// ahead in the queue block new requests, which keeps writers from starving.
func (lm *LockManager) grantable(e *lockEntry, id ID, mode Mode) bool {
	for h, hm := range e.holders {
		if h == id {
			continue
		}
		if !compatible(hm, mode) {
			return false
		}
	}
	if len(e.waiters) > 0 && mode == LockShared {
		// Only jump the queue if every waiter is also a reader.
		for _, w := range e.waiters {
			if w.mode == LockExclusive {
				return false
			}
		}
	}
	return len(e.waiters) == 0 || mode == LockShared
}

// waitFor enqueues the request and blocks. Called with lm.mu held; it always
// releases it.
func (lm *LockManager) waitFor(t *Txn, key Key, mode Mode, e *lockEntry) error {
	w := &waiter{txn: t, mode: mode, ch: make(chan error, 1)}
	e.waiters = append(e.waiters, w)
	lm.waitingOn[t.ID()] = key

	if victim := lm.detectDeadlock(t.ID()); victim != 0 {
		lm.deadlocks++
		if victim == t.ID() {
			lm.removeWaiter(e, w)
			delete(lm.waitingOn, t.ID())
			lm.mu.Unlock()
			t.markAborting()
			return ErrDeadlock
		}
		lm.abortVictimLocked(victim)
	}
	lm.mu.Unlock()

	select {
	case err := <-w.ch:
		return err
	case <-time.After(lm.timeout):
		lm.mu.Lock()
		// The grant may have arrived while the timer was firing.
		select {
		case err := <-w.ch:
			lm.mu.Unlock()
			return err
		default:
		}
		lm.removeWaiter(e, w)
		delete(lm.waitingOn, t.ID())
		lm.mu.Unlock()
		return ErrLockTimeout
	}
}

func (lm *LockManager) removeWaiter(e *lockEntry, w *waiter) {
	for i, x := range e.waiters {
		if x == w {
			e.waiters = append(e.waiters[:i], e.waiters[i+1:]...)
			return
		}
	}
}

// abortVictimLocked marks a transaction as doomed and, if it is blocked, wakes
// it up with ErrDeadlock.
func (lm *LockManager) abortVictimLocked(victim ID) {
	if t := lm.txns[victim]; t != nil {
		t.markAborting()
	}
	key, waiting := lm.waitingOn[victim]
	if !waiting {
		return // it is running; it will notice when it next asks for a lock
	}
	e := lm.table[key]
	if e == nil {
		return
	}
	for i, w := range e.waiters {
		if w.txn.ID() == victim {
			e.waiters = append(e.waiters[:i], e.waiters[i+1:]...)
			delete(lm.waitingOn, victim)
			w.ch <- ErrDeadlock
			return
		}
	}
}
