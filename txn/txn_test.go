package txn

import (
	"path/filepath"
	"testing"
	"time"

	"minidb/storage"
	"minidb/wal"
)

func newManager(t testing.TB) *Manager {
	t.Helper()
	lg, err := wal.Open(filepath.Join(t.TempDir(), "t.wal"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { lg.Close() })
	return NewManager(lg, 2*time.Second, 1)
}

func TestBeginCommitAbort(t *testing.T) {
	m := newManager(t)
	t1, err := m.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if t1.State() != StateGrowing {
		t.Fatalf("state = %v", t1.State())
	}
	if m.ActiveCount() != 1 {
		t.Fatalf("active = %d", m.ActiveCount())
	}
	if err := m.Commit(t1); err != nil {
		t.Fatal(err)
	}
	if t1.State() != StateCommitted || m.ActiveCount() != 0 {
		t.Fatalf("after commit: %v active=%d", t1.State(), m.ActiveCount())
	}

	t2, _ := m.Begin()
	if err := m.Abort(t2); err != nil {
		t.Fatal(err)
	}
	if t2.State() != StateAborted {
		t.Fatalf("after abort: %v", t2.State())
	}
	// Transaction ids increase, so the deadlock detector can order them.
	if t2.ID() <= t1.ID() {
		t.Fatalf("ids not increasing: %d then %d", t1.ID(), t2.ID())
	}
}

func TestSharedLocksAreShared(t *testing.T) {
	m := newManager(t)
	lm := m.Locks()
	key := RowKey(1, storage.RID{PageID: 1, Slot: 0})

	a, _ := m.Begin()
	b, _ := m.Begin()
	if err := lm.Acquire(a, key, LockShared); err != nil {
		t.Fatal(err)
	}
	if err := lm.Acquire(b, key, LockShared); err != nil {
		t.Fatal("two readers must be able to share a row lock:", err)
	}
	m.Commit(a)
	m.Commit(b)
}

func TestExclusiveLockBlocksUntilCommit(t *testing.T) {
	m := newManager(t)
	lm := m.Locks()
	key := RowKey(1, storage.RID{PageID: 1, Slot: 0})

	a, _ := m.Begin()
	if err := lm.Acquire(a, key, LockExclusive); err != nil {
		t.Fatal(err)
	}

	got := make(chan error, 1)
	b, _ := m.Begin()
	go func() { got <- lm.Acquire(b, key, LockShared) }()

	select {
	case err := <-got:
		t.Fatalf("reader was let through while a writer held the row: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	// Strict 2PL: the lock is only released at end of transaction.
	m.Commit(a)
	select {
	case err := <-got:
		if err != nil {
			t.Fatalf("waiter got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("waiter was never woken after the holder committed")
	}
	m.Commit(b)
}

func TestLockUpgrade(t *testing.T) {
	m := newManager(t)
	lm := m.Locks()
	key := RowKey(1, storage.RID{PageID: 2, Slot: 3})
	a, _ := m.Begin()
	if err := lm.Acquire(a, key, LockShared); err != nil {
		t.Fatal(err)
	}
	if err := lm.Acquire(a, key, LockExclusive); err != nil {
		t.Fatalf("sole shared holder should be able to upgrade: %v", err)
	}
	// Re-acquiring something weaker is a no-op, not a downgrade.
	if err := lm.Acquire(a, key, LockShared); err != nil {
		t.Fatal(err)
	}
	m.Commit(a)
}

func TestDeadlockIsDetectedAndBroken(t *testing.T) {
	m := newManager(t)
	lm := m.Locks()
	k1 := RowKey(1, storage.RID{PageID: 1, Slot: 0})
	k2 := RowKey(1, storage.RID{PageID: 1, Slot: 1})

	a, _ := m.Begin()
	b, _ := m.Begin()
	if err := lm.Acquire(a, k1, LockExclusive); err != nil {
		t.Fatal(err)
	}
	if err := lm.Acquire(b, k2, LockExclusive); err != nil {
		t.Fatal(err)
	}

	// a wants what b holds, b wants what a holds: a textbook cycle.
	errA := make(chan error, 1)
	errB := make(chan error, 1)
	go func() { errA <- lm.Acquire(a, k2, LockExclusive) }()
	time.Sleep(30 * time.Millisecond) // make the order deterministic
	go func() { errB <- lm.Acquire(b, k1, LockExclusive) }()

	var victims int
	deadline := time.After(3 * time.Second)
	for i := 0; i < 2; i++ {
		select {
		case err := <-errA:
			if err == ErrDeadlock {
				victims++
				m.Abort(a)
			}
		case err := <-errB:
			if err == ErrDeadlock {
				victims++
				m.Abort(b)
			}
		case <-deadline:
			t.Fatal("deadlock was never broken: both transactions are stuck")
		}
		if victims > 0 {
			break
		}
	}
	if victims == 0 {
		t.Fatal("no transaction was aborted")
	}
	if lm.Deadlocks() == 0 {
		t.Fatal("deadlock not counted")
	}
}
