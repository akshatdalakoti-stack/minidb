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
