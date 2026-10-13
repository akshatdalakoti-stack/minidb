package index

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"path/filepath"
	"sort"
	"sync"
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

func TestTreeDelete(t *testing.T) {
	h := newHarness(t, 64)
	tr := newTree(t, h)

	const n = 5000
	for i := 0; i < n; i++ {
		if err := tr.Insert(h.ctx, key(i), rid(i)); err != nil {
			t.Fatal(err)
		}
	}
	// Delete every other key.
	for i := 0; i < n; i += 2 {
		ok, err := tr.Delete(h.ctx, key(i), rid(i))
		if err != nil {
			t.Fatalf("delete %d: %v", i, err)
		}
		if !ok {
			t.Fatalf("delete %d reported not found", i)
		}
	}
	if err := tr.Validate(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		got, err := tr.Search(key(i))
		if err != nil {
			t.Fatal(err)
		}
		want := 1
		if i%2 == 0 {
			want = 0
		}
		if len(got) != want {
			t.Fatalf("after deletes, key %d has %d entries, want %d", i, len(got), want)
		}
	}
	// Deleting something that is not there is not an error, just false.
	if ok, err := tr.Delete(h.ctx, key(0), rid(0)); err != nil || ok {
		t.Fatalf("re-delete: ok=%v err=%v", ok, err)
	}

	// Now delete the rest: the tree must collapse back towards a single leaf.
	for i := 1; i < n; i += 2 {
		if _, err := tr.Delete(h.ctx, key(i), rid(i)); err != nil {
			t.Fatalf("delete %d: %v", i, err)
		}
	}
	if err := tr.Validate(); err != nil {
		t.Fatal(err)
	}
	if c, _ := tr.Count(); c != 0 {
		t.Fatalf("tree not empty after deleting everything: %d entries", c)
	}
	height, _ := tr.Height()
	if height != 1 {
		t.Fatalf("emptied tree still has height %d", height)
	}
	// And it still works afterwards.
	if err := tr.Insert(h.ctx, key(42), rid(42)); err != nil {
		t.Fatal(err)
	}
	if got, _ := tr.Search(key(42)); len(got) != 1 {
		t.Fatal("tree unusable after being emptied")
	}
}

func TestTreeRangeScan(t *testing.T) {
	h := newHarness(t, 64)
	tr := newTree(t, h)

	const n = 3000
	for i := 0; i < n; i++ {
		if err := tr.Insert(h.ctx, key(i), rid(i)); err != nil {
			t.Fatal(err)
		}
	}
	check := func(start, end []byte, wantFirst, wantLast, wantCount int) {
		t.Helper()
		it := tr.Scan(start, end)
		var keys []int
		for it.Next() {
			keys = append(keys, int(binary.BigEndian.Uint64(it.Key())))
		}
		if err := it.Err(); err != nil {
			t.Fatal(err)
		}
		if len(keys) != wantCount {
			t.Fatalf("scan returned %d keys, want %d", len(keys), wantCount)
		}
		if wantCount == 0 {
			return
		}
		if keys[0] != wantFirst || keys[len(keys)-1] != wantLast {
			t.Fatalf("scan range [%d..%d], want [%d..%d]", keys[0], keys[len(keys)-1], wantFirst, wantLast)
		}
		if !sort.IntsAreSorted(keys) {
			t.Fatal("scan results are not in key order")
		}
	}
	check(nil, nil, 0, n-1, n)               // everything
	check(key(100), key(200), 100, 199, 100) // [100,200)
	check(key(0), key(1), 0, 0, 1)           // single key
	check(key(n), nil, 0, 0, 0)              // past the end
	check(key(2990), nil, 2990, n-1, 10)     // open ended
}

func TestTreeScanFollowsLeafChainAfterSplits(t *testing.T) {
	// The leaf chain is what makes a range scan cheap; this checks it stays
	// intact through hundreds of splits.
	h := newHarness(t, 32)
	tr := newTree(t, h)
	const n = 10000
	rng := rand.New(rand.NewSource(1))
	for _, i := range rng.Perm(n) {
		if err := tr.Insert(h.ctx, key(i), rid(i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := tr.Validate(); err != nil {
		t.Fatal(err)
	}
	it := tr.Scan(nil, nil)
	prev := -1
	count := 0
	for it.Next() {
		k := int(binary.BigEndian.Uint64(it.Key()))
		if k <= prev {
			t.Fatalf("scan out of order: %d after %d", k, prev)
		}
		prev, count = k, count+1
	}
	if it.Err() != nil {
		t.Fatal(it.Err())
	}
	if count != n {
		t.Fatalf("scan visited %d of %d entries", count, n)
	}
}

func TestTreeVariableLengthKeys(t *testing.T) {
	h := newHarness(t, 64)
	tr := newTree(t, h)

	words := make([]string, 0, 2000)
	rng := rand.New(rand.NewSource(3))
	for i := 0; i < 2000; i++ {
		n := 1 + rng.Intn(60)
		b := make([]byte, n)
		for j := range b {
			b[j] = byte('a' + rng.Intn(26))
		}
		words = append(words, fmt.Sprintf("%s-%d", b, i))
	}
	for i, w := range words {
		if err := tr.Insert(h.ctx, []byte(w), rid(i)); err != nil {
			t.Fatalf("insert %q: %v", w, err)
		}
	}
	if err := tr.Validate(); err != nil {
		t.Fatal(err)
	}
	for i, w := range words {
		got, err := tr.Search([]byte(w))
		if err != nil || len(got) != 1 || got[0] != rid(i) {
			t.Fatalf("search %q = %v (%v)", w, got, err)
		}
	}
	// Sorted order must be byte order.
	sorted := append([]string(nil), words...)
	sort.Strings(sorted)
	it := tr.Scan(nil, nil)
	i := 0
	for it.Next() {
		if string(it.Key()) != sorted[i] {
			t.Fatalf("position %d: got %q want %q", i, it.Key(), sorted[i])
		}
		i++
	}
	if i != len(sorted) {
		t.Fatalf("scanned %d of %d", i, len(sorted))
	}
}

func TestTreeKeyTooLarge(t *testing.T) {
	h := newHarness(t, 16)
	tr := newTree(t, h)
	big := bytes.Repeat([]byte("k"), MaxKeySize+1)
	if err := tr.Insert(h.ctx, big, rid(1)); err == nil {
		t.Fatal("oversized key was accepted")
	}
}

func TestTreeSurvivesReopen(t *testing.T) {
	h := newHarness(t, 16)
	tr := newTree(t, h)
	meta := tr.MetaPage()
	for i := 0; i < 2000; i++ {
		if err := tr.Insert(h.ctx, key(i), rid(i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := h.pool.FlushAllPages(); err != nil {
		t.Fatal(err)
	}
	// A new Tree object over the same meta page must see the same data - the
	// tree lives on disk, not in the struct.
	tr2 := Open(h.pool, meta, "test_idx")
	if c, _ := tr2.Count(); c != 2000 {
		t.Fatalf("reopened tree has %d entries", c)
	}
	got, err := tr2.Search(key(1234))
	if err != nil || len(got) != 1 {
		t.Fatalf("search after reopen: %v %v", got, err)
	}
}

func TestTreeSmallBufferPool(t *testing.T) {
	// Four frames: every descent evicts something. This is the test that
	// catches "I forgot to unpin" bugs.
	h := newHarness(t, 4)
	tr := newTree(t, h)
	for i := 0; i < 3000; i++ {
		if err := tr.Insert(h.ctx, key(i), rid(i)); err != nil {
			t.Fatalf("insert %d with a tiny pool: %v", i, err)
		}
	}
	if err := tr.Validate(); err != nil {
		t.Fatal(err)
	}
	if s := h.pool.Stats(); s.Pinned != 0 {
		t.Fatalf("pins leaked: %v", s)
	}
	for i := 0; i < 3000; i += 97 {
		if got, _ := tr.Search(key(i)); len(got) != 1 {
			t.Fatalf("key %d missing", i)
		}
	}
}

func TestTreeConcurrentReadersAndWriters(t *testing.T) {
	h := newHarness(t, 64)
	tr := newTree(t, h)
	for i := 0; i < 2000; i++ {
		if err := tr.Insert(h.ctx, key(i), rid(i)); err != nil {
			t.Fatal(err)
		}
	}

	var wg sync.WaitGroup
	// Writers insert disjoint ranges.
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				k := 10000 + w*1000 + i
				if err := tr.Insert(h.ctx, key(k), rid(k)); err != nil {
					t.Errorf("concurrent insert: %v", err)
					return
				}
			}
		}(w)
	}
	// Readers search and scan while that happens.
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func(r int) {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				k := (r*137 + i) % 2000
				got, err := tr.Search(key(k))
				if err != nil {
					t.Errorf("concurrent search: %v", err)
					return
				}
				if len(got) != 1 {
					t.Errorf("key %d lost during concurrent inserts: %v", k, got)
					return
				}
			}
			it := tr.Scan(key(0), key(2000))
			n := 0
			for it.Next() {
				n++
			}
			if it.Err() != nil {
				t.Errorf("concurrent scan: %v", it.Err())
			}
			if n != 2000 {
				t.Errorf("concurrent scan saw %d of the 2000 pre-existing keys", n)
			}
		}(r)
	}
	wg.Wait()
	if err := tr.Validate(); err != nil {
		t.Fatal(err)
	}
	if c, _ := tr.Count(); c != 4000 {
		t.Fatalf("count = %d want 4000", c)
	}
}
