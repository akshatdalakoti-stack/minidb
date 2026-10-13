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

func TestHeapUpdateInPlaceAndRelocation(t *testing.T) {
	h := newHarness(t, 16)
	hp := newHeap(t, h)

	rid, err := hp.Insert(h.ctx, storage.Record("small"))
	if err != nil {
		t.Fatal(err)
	}
	// Same size or smaller: the RID must not move, because indexes point at it.
	newRID, err := hp.Update(h.ctx, rid, storage.Record("tiny"))
	if err != nil {
		t.Fatal(err)
	}
	if newRID != rid {
		t.Fatalf("shrinking update moved the record from %v to %v", rid, newRID)
	}
	if got, _ := hp.Get(rid); string(got) != "tiny" {
		t.Fatalf("got %q", got)
	}

	// Fill the page so that a growing update cannot stay put.
	filler := bytes.Repeat([]byte("f"), 500)
	for {
		st, _ := hp.Stats()
		if st.Pages > 1 {
			break
		}
		if _, err := hp.Insert(h.ctx, filler); err != nil {
			t.Fatal(err)
		}
	}
	big := bytes.Repeat([]byte("B"), 2000)
	moved, err := hp.Update(h.ctx, rid, big)
	if err != nil {
		t.Fatal(err)
	}
	if moved == rid {
		t.Log("record happened to fit in place")
	} else {
		if _, err := hp.Get(rid); err == nil {
			t.Fatal("old RID still resolves after relocation")
		}
	}
	got, err := hp.Get(moved)
	if err != nil || !bytes.Equal(got, big) {
		t.Fatalf("relocated record: %d bytes, %v", len(got), err)
	}
}

func TestHeapScan(t *testing.T) {
	h := newHarness(t, 16)
	hp := newHeap(t, h)

	const n = 2000
	want := map[string]bool{}
	rids := make([]storage.RID, n)
	for i := 0; i < n; i++ {
		rid, _ := hp.Insert(h.ctx, rec(i))
		rids[i] = rid
		want[string(rec(i))] = true
	}
	// Delete a few: a scan must skip tombstones.
	for i := 0; i < n; i += 7 {
		hp.Delete(h.ctx, rids[i])
		delete(want, string(rec(i)))
	}

	seen := map[string]bool{}
	s := hp.Scan()
	for s.Next() {
		row := s.Row()
		if seen[string(row.Record)] {
			t.Fatalf("scan returned %q twice", row.Record)
		}
		seen[string(row.Record)] = true
		// The RID a scan reports must resolve to the same record.
		got, err := hp.Get(row.RID)
		if err != nil || !bytes.Equal(got, row.Record) {
			t.Fatalf("scan RID %v does not resolve: %q %v", row.RID, got, err)
		}
	}
	if s.Err() != nil {
		t.Fatal(s.Err())
	}
	if len(seen) != len(want) {
		t.Fatalf("scan saw %d records, want %d", len(seen), len(want))
	}
	for k := range want {
		if !seen[k] {
			t.Fatalf("scan missed %q", k)
		}
	}
}

func TestHeapReopen(t *testing.T) {
	h := newHarness(t, 16)
	hp := newHeap(t, h)
	meta := hp.MetaPage()
	for i := 0; i < 3000; i++ {
		if _, err := hp.Insert(h.ctx, rec(i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := h.pool.FlushAllPages(); err != nil {
		t.Fatal(err)
	}
	hp2, err := Open(h.pool, meta, "users", 1)
	if err != nil {
		t.Fatal(err)
	}
	n, err := hp2.Count()
	if err != nil {
		t.Fatal(err)
	}
	if n != 3000 {
		t.Fatalf("reopened heap has %d records", n)
	}
	// And it can still be appended to, on the tail page it recovered.
	if _, err := hp2.Insert(h.ctx, rec(999999)); err != nil {
		t.Fatal(err)
	}
}
