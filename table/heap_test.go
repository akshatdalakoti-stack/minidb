package table

import (
	"bytes"
	"fmt"
	"path/filepath"
	"testing"

	"minidb/buffer"
	"minidb/pageops"
	"minidb/storage"
	"minidb/wal"
)

type harness struct {
	pool *buffer.Pool
	ctx  pageops.Ctx
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
	lg.SetNoSync(true)
	pool := buffer.NewPool(frames, dm, nil)
	pool.SetLogFlusher(func(lsn uint64) error { return lg.Flush(lsn + 1) })
	t.Cleanup(func() {
		pool.FlushAllPages()
		lg.Close()
		dm.Close()
	})
	return &harness{pool: pool, ctx: pageops.Ctx{Pool: pool, Log: lg}}
}

func newHeap(t testing.TB, h *harness) *Heap {
	t.Helper()
	hp, err := Create(h.ctx, "users", 1)
	if err != nil {
		t.Fatal(err)
	}
	return hp
}

func rec(i int) storage.Record { return storage.Record(fmt.Sprintf("record-number-%06d", i)) }

func TestHeapInsertGet(t *testing.T) {
	h := newHarness(t, 32)
	hp := newHeap(t, h)

	rids := make([]storage.RID, 0, 100)
	for i := 0; i < 100; i++ {
		rid, err := hp.Insert(h.ctx, rec(i))
		if err != nil {
			t.Fatal(err)
		}
		rids = append(rids, rid)
	}
	for i, rid := range rids {
		got, err := hp.Get(rid)
		if err != nil {
			t.Fatalf("get %v: %v", rid, err)
		}
		if !bytes.Equal(got, rec(i)) {
			t.Fatalf("got %q want %q", got, rec(i))
		}
	}
	if _, err := hp.Get(storage.RID{PageID: 1, Slot: 9999}); err == nil {
		t.Fatal("get of a nonexistent slot succeeded")
	}
	if n, _ := hp.Count(); n != 100 {
		t.Fatalf("count = %d", n)
	}
}

func TestHeapGrowsAcrossPages(t *testing.T) {
	h := newHarness(t, 16)
	hp := newHeap(t, h)

	const n = 5000
	rids := make([]storage.RID, n)
	for i := 0; i < n; i++ {
		rid, err := hp.Insert(h.ctx, rec(i))
		if err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
		rids[i] = rid
	}
	st, err := hp.Stats()
	if err != nil {
		t.Fatal(err)
	}
	if st.Pages < 2 {
		t.Fatalf("expected the heap to span pages, got %d", st.Pages)
	}
	if st.Records != n {
		t.Fatalf("stats say %d records, want %d", st.Records, n)
	}
	t.Logf("%d records in %d pages, fill=%.2f", st.Records, st.Pages, st.Fill())
	for i := 0; i < n; i += 173 {
		got, err := hp.Get(rids[i])
		if err != nil || !bytes.Equal(got, rec(i)) {
			t.Fatalf("record %d: %q %v", i, got, err)
		}
	}
}

func TestHeapDeleteAndReuse(t *testing.T) {
	h := newHarness(t, 16)
	hp := newHeap(t, h)

	rids := make([]storage.RID, 0, 1000)
	for i := 0; i < 1000; i++ {
		rid, _ := hp.Insert(h.ctx, rec(i))
		rids = append(rids, rid)
	}
	before, _ := hp.Stats()

	for i := 0; i < 1000; i += 2 {
		if err := hp.Delete(h.ctx, rids[i]); err != nil {
			t.Fatalf("delete %d: %v", i, err)
		}
	}
	if n, _ := hp.Count(); n != 500 {
		t.Fatalf("count after deletes = %d", n)
	}
	// A deleted record is gone, its neighbours are not.
	if _, err := hp.Get(rids[0]); err == nil {
		t.Fatal("deleted record is still readable")
	}
	if got, err := hp.Get(rids[1]); err != nil || !bytes.Equal(got, rec(1)) {
		t.Fatalf("neighbour damaged: %q %v", got, err)
	}
	if err := hp.Delete(h.ctx, rids[0]); err == nil {
		t.Fatal("double delete succeeded")
	}

	// Re-inserting should reuse the freed space rather than growing the file.
	for i := 0; i < 400; i++ {
		if _, err := hp.Insert(h.ctx, rec(10000+i)); err != nil {
			t.Fatal(err)
		}
	}
	after, _ := hp.Stats()
	if after.Pages > before.Pages {
		t.Fatalf("heap grew from %d to %d pages instead of reusing freed space", before.Pages, after.Pages)
	}
}
