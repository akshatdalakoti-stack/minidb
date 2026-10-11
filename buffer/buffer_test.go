package buffer

import (
	"fmt"
	"path/filepath"
	"testing"

	"minidb/storage"
)

func newTestPool(t testing.TB, frames int, r Replacer) (*Pool, *storage.DiskManager) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "buf.db")
	dm, err := storage.OpenDiskManager(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { dm.Close() })
	return NewPool(frames, dm, r), dm
}

// writePage allocates a page and stores s in it.
func writePage(t testing.TB, bp *Pool, s string) storage.PageID {
	t.Helper()
	id, p, err := bp.NewPage()
	if err != nil {
		t.Fatal(err)
	}
	p.InitSlotted(storage.PageTypeHeap)
	if _, err := p.Insert([]byte(s)); err != nil {
		t.Fatal(err)
	}
	if err := bp.UnpinPage(id, true); err != nil {
		t.Fatal(err)
	}
	return id
}

func readPage(t testing.TB, bp *Pool, id storage.PageID) string {
	t.Helper()
	p, err := bp.FetchPage(id)
	if err != nil {
		t.Fatalf("fetch %v: %v", id, err)
	}
	rec, err := p.Get(0)
	if err != nil {
		t.Fatalf("get from %v: %v", id, err)
	}
	s := string(rec)
	if err := bp.UnpinPage(id, false); err != nil {
		t.Fatal(err)
	}
	return s
}

func TestPoolFetchHitAndMiss(t *testing.T) {
	bp, _ := newTestPool(t, 4, nil)
	id := writePage(t, bp, "hello")

	bp.ResetStats()
	if got := readPage(t, bp, id); got != "hello" {
		t.Fatalf("got %q", got)
	}
	if s := bp.Stats(); s.Hits != 1 || s.Misses != 0 {
		t.Fatalf("expected a hit: %v", s)
	}
	// Force it out of the pool, then read again: that must be a miss served
	// from disk with the same contents.
	for i := 0; i < 8; i++ {
		writePage(t, bp, fmt.Sprintf("filler-%d", i))
	}
	bp.ResetStats()
	if got := readPage(t, bp, id); got != "hello" {
		t.Fatalf("after eviction got %q", got)
	}
	if s := bp.Stats(); s.Misses != 1 {
		t.Fatalf("expected a miss: %v", s)
	}
}

func TestPoolPinnedPagesAreNeverEvicted(t *testing.T) {
	bp, _ := newTestPool(t, 3, nil)
	bp.SetFrameWaitTimeout(0) // fail fast instead of waiting for an unpin
	var ids []storage.PageID
	for i := 0; i < 3; i++ {
		id, p, err := bp.NewPage()
		if err != nil {
			t.Fatal(err)
		}
		p.InitSlotted(storage.PageTypeHeap)
		p.Insert([]byte(fmt.Sprintf("page-%d", i)))
		ids = append(ids, id) // deliberately left pinned
	}
	// Every frame is pinned: a fourth page cannot be brought in.
	if _, _, err := bp.NewPage(); err != ErrNoFreeFrame {
		t.Fatalf("expected ErrNoFreeFrame, got %v", err)
	}
	for _, id := range ids {
		if bp.PinCount(id) != 1 {
			t.Fatalf("page %v pin count = %d", id, bp.PinCount(id))
		}
		if err := bp.UnpinPage(id, true); err != nil {
			t.Fatal(err)
		}
	}
	// Now there is room again.
	if _, _, err := bp.NewPage(); err != nil {
		t.Fatalf("after unpinning: %v", err)
	}
}

func TestPoolOverUnpin(t *testing.T) {
	bp, _ := newTestPool(t, 4, nil)
	id := writePage(t, bp, "x")
	if err := bp.UnpinPage(id, false); err != ErrOverUnpin {
		t.Fatalf("want ErrOverUnpin got %v", err)
	}
	if err := bp.UnpinPage(9999, false); err != ErrPageNotInUse {
		t.Fatalf("want ErrPageNotInUse got %v", err)
	}
}

func TestPoolDirtyEvictionWritesBack(t *testing.T) {
	bp, dm := newTestPool(t, 2, nil)
	id := writePage(t, bp, "original")

	// Modify it and unpin dirty.
	p, err := bp.FetchPage(id)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Update(0, []byte("modified")); err != nil {
		t.Fatal(err)
	}
	bp.UnpinPage(id, true)
	if !bp.IsDirty(id) {
		t.Fatal("page should be dirty")
	}

	// Push it out of the pool.
	for i := 0; i < 4; i++ {
		writePage(t, bp, "filler")
	}
	if bp.IsResident(id) {
		t.Fatal("page should have been evicted")
	}
	if bp.Stats().DirtyEvic == 0 {
		t.Fatal("dirty eviction not counted")
	}

	// Read straight from disk, bypassing the pool: the change must be there.
	var raw storage.Page
	if err := dm.ReadPage(id, &raw); err != nil {
		t.Fatal(err)
	}
	got, err := raw.Get(0)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "modified" {
		t.Fatalf("disk has %q, dirty page was lost", got)
	}
}

func TestPoolFlushAndWALHook(t *testing.T) {
	bp, dm := newTestPool(t, 4, nil)
	var flushedTo uint64
	var calls int
	bp.SetLogFlusher(func(lsn uint64) error {
		calls++
		flushedTo = lsn
		return nil
	})

	id, p, err := bp.NewPage()
	if err != nil {
		t.Fatal(err)
	}
	p.InitSlotted(storage.PageTypeHeap)
	p.Insert([]byte("logged"))
	p.SetLSN(77)
	bp.UnpinPage(id, true)

	if err := bp.FlushAllPages(); err != nil {
		t.Fatal(err)
	}
	if calls == 0 || flushedTo != 77 {
		t.Fatalf("WAL hook not honoured: calls=%d lsn=%d", calls, flushedTo)
	}
	if bp.IsDirty(id) {
		t.Fatal("page still dirty after flush")
	}
	var raw storage.Page
	if err := dm.ReadPage(id, &raw); err != nil {
		t.Fatal(err)
	}
	if got, _ := raw.Get(0); string(got) != "logged" {
		t.Fatalf("flush did not reach disk: %q", got)
	}
}

func TestPoolDeletePage(t *testing.T) {
	bp, _ := newTestPool(t, 4, nil)
	id, _, err := bp.NewPage()
	if err != nil {
		t.Fatal(err)
	}
	if err := bp.DeletePage(id); err != ErrPagePinned {
		t.Fatalf("want ErrPagePinned got %v", err)
	}
	bp.UnpinPage(id, false)
	if err := bp.DeletePage(id); err != nil {
		t.Fatal(err)
	}
	if bp.IsResident(id) {
		t.Fatal("deleted page still resident")
	}
	// The page id should come back from the free list.
	id2, _, err := bp.NewPage()
	if err != nil {
		t.Fatal(err)
	}
	if id2 != id {
		t.Fatalf("freed page %v not reused (got %v)", id, id2)
	}
}

// --- Phase 4: replacement policies ----------------------------------------

func TestLRUEvictsLeastRecentlyUsed(t *testing.T) {
	bp, _ := newTestPool(t, 3, NewLRUReplacer(3))
	a := writePage(t, bp, "A")
	b := writePage(t, bp, "B")
	c := writePage(t, bp, "C")

	readPage(t, bp, a) // touch A, so B is now the least recently used
	d := writePage(t, bp, "D")

	if bp.IsResident(b) {
		t.Fatal("LRU evicted the wrong page: B should be gone")
	}
	if !bp.IsResident(a) || !bp.IsResident(c) || !bp.IsResident(d) {
		t.Fatal("LRU evicted more than it needed to")
	}
}

func TestReplacerContracts(t *testing.T) {
	policies := []struct {
		name string
		make func(n int) Replacer
	}{
		{"LRU", func(n int) Replacer { return NewLRUReplacer(n) }},
		{"Clock", func(n int) Replacer { return NewClockReplacer(n) }},
		{"LRU-K", func(n int) Replacer { return NewLRUKReplacer(n, 2) }},
	}
	for _, pol := range policies {
		t.Run(pol.name, func(t *testing.T) {
			r := pol.make(4)
			if _, ok := r.Evict(); ok {
				t.Fatal("empty replacer produced a victim")
			}
			for f := 0; f < 4; f++ {
				r.RecordAccess(f)
				r.SetEvictable(f, false) // all pinned
			}
			if r.Size() != 0 {
				t.Fatalf("size = %d with everything pinned", r.Size())
			}
			if _, ok := r.Evict(); ok {
				t.Fatal("evicted a pinned frame")
			}
			r.SetEvictable(2, true)
			if r.Size() != 1 {
				t.Fatalf("size = %d", r.Size())
			}
			f, ok := r.Evict()
			if !ok || f != 2 {
				t.Fatalf("expected frame 2, got %d ok=%v", f, ok)
			}
			if r.Size() != 0 {
				t.Fatalf("size after evict = %d", r.Size())
			}
		})
	}
}

func TestLRUKPrefersFramesSeenOnlyOnce(t *testing.T) {
	// k=2: frames touched once have infinite backward k-distance and go first,
	// which is what protects a hot working set from a sequential scan.
	r := NewLRUKReplacer(3, 2)
	r.RecordAccess(0)
	r.RecordAccess(0) // frame 0 is "hot": two references
	r.RecordAccess(1) // frame 1 seen once
	r.RecordAccess(2)
	r.RecordAccess(2)
	for f := 0; f < 3; f++ {
		r.SetEvictable(f, true)
	}
	f, ok := r.Evict()
	if !ok || f != 1 {
		t.Fatalf("LRU-K should evict the single-reference frame 1, got %d", f)
	}
}

func TestClockGivesSecondChance(t *testing.T) {
	r := NewClockReplacer(3)
	for f := 0; f < 3; f++ {
		r.RecordAccess(f)
		r.SetEvictable(f, true)
	}
	// Every frame has its reference bit set, so the first sweep clears them
	// and the second picks the hand position: frame 0.
	f, ok := r.Evict()
	if !ok || f != 0 {
		t.Fatalf("got frame %d ok=%v", f, ok)
	}
	// Re-reference frame 1 so it survives the next eviction.
	r.RecordAccess(1)
	f, ok = r.Evict()
	if !ok || f != 2 {
		t.Fatalf("re-referenced frame was not spared: victim %d", f)
	}
}
