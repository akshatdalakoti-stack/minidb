package index

import (
	"bytes"
	"encoding/binary"
	"math/rand"
	"path/filepath"
	"testing"

	"minidb/buffer"
	"minidb/pageops"
	"minidb/storage"
	"minidb/wal"
)

type harness struct {
	pool *buffer.Pool
	log  *wal.Manager
	ctx  pageops.Ctx
	dir  string
}

func newHarness(t testing.TB, frames int) *harness {
	t.Helper()
	dir := t.TempDir()
	dm, err := storage.OpenDiskManager(filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	lg, err := wal.Open(filepath.Join(dir, "test.wal"))
	if err != nil {
		t.Fatal(err)
	}
	lg.SetNoSync(true) // these tests are about the tree, not about fsync latency
	pool := buffer.NewPool(frames, dm, nil)
	pool.SetLogFlusher(func(lsn uint64) error { return lg.Flush(lsn + 1) })
	t.Cleanup(func() {
		pool.FlushAllPages()
		lg.Close()
		dm.Close()
	})
	return &harness{pool: pool, log: lg, ctx: pageops.Ctx{Pool: pool, Log: lg}, dir: dir}
}

// key encodes an int as an order-preserving 8 byte key (big endian, so
// bytes.Compare and integer comparison agree).
func key(i int) []byte {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(i))
	return b[:]
}

func rid(i int) storage.RID {
	return storage.RID{PageID: storage.PageID(i / 100), Slot: uint16(i % 100)}
}

func newTree(t testing.TB, h *harness) *Tree {
	t.Helper()
	tr, err := Create(h.ctx, "test_idx")
	if err != nil {
		t.Fatal(err)
	}
	return tr
}

func TestTreeInsertAndSearch(t *testing.T) {
	h := newHarness(t, 32)
	tr := newTree(t, h)

	for i := 0; i < 100; i++ {
		if err := tr.Insert(h.ctx, key(i), rid(i)); err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}
	for i := 0; i < 100; i++ {
		got, err := tr.Search(key(i))
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 1 || got[0] != rid(i) {
			t.Fatalf("search %d = %v", i, got)
		}
	}
	if got, _ := tr.Search(key(1000)); len(got) != 0 {
		t.Fatalf("search for a missing key returned %v", got)
	}
	if err := tr.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestTreeSplitsAndGrows(t *testing.T) {
	h := newHarness(t, 64)
	tr := newTree(t, h)

	const n = 20000
	for i := 0; i < n; i++ {
		if err := tr.Insert(h.ctx, key(i), rid(i)); err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}
	if err := tr.Validate(); err != nil {
		t.Fatal(err)
	}
	height, err := tr.Height()
	if err != nil {
		t.Fatal(err)
	}
	if height < 2 {
		t.Fatalf("tree never split: height %d after %d keys", height, n)
	}
	if c, _ := tr.Count(); c != n {
		t.Fatalf("count = %d want %d", c, n)
	}
	// Spot check across the whole range.
	for _, i := range []int{0, 1, n / 3, n / 2, n - 2, n - 1} {
		got, err := tr.Search(key(i))
		if err != nil || len(got) != 1 || got[0] != rid(i) {
			t.Fatalf("search %d = %v (%v)", i, got, err)
		}
	}
	t.Logf("height=%d for %d keys", height, n)
}

func TestTreeRandomInsertOrder(t *testing.T) {
	h := newHarness(t, 64)
	tr := newTree(t, h)

	rng := rand.New(rand.NewSource(42))
	perm := rng.Perm(5000)
	for _, i := range perm {
		if err := tr.Insert(h.ctx, key(i), rid(i)); err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}
	if err := tr.Validate(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5000; i++ {
		got, err := tr.Search(key(i))
		if err != nil || len(got) != 1 {
			t.Fatalf("search %d = %v (%v)", i, got, err)
		}
	}
}

func TestTreeDuplicateKeys(t *testing.T) {
	h := newHarness(t, 32)
	tr := newTree(t, h)

	// A non-unique index: one key, many records.
	for i := 0; i < 500; i++ {
		if err := tr.Insert(h.ctx, key(7), rid(i)); err != nil {
			t.Fatal(err)
		}
	}
	tr.Insert(h.ctx, key(6), rid(9999))
	tr.Insert(h.ctx, key(8), rid(9998))

	got, err := tr.Search(key(7))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 500 {
		t.Fatalf("got %d rids for the duplicated key, want 500", len(got))
	}
	// Inserting the exact same (key, rid) twice must be a no-op: recovery
	// replays log records, and a replay must not double-insert.
	if err := tr.Insert(h.ctx, key(7), rid(0)); err != nil {
		t.Fatal(err)
	}
	if got, _ := tr.Search(key(7)); len(got) != 500 {
		t.Fatalf("duplicate (key,rid) inserted twice: %d entries", len(got))
	}
	if err := tr.Validate(); err != nil {
		t.Fatal(err)
	}
}
