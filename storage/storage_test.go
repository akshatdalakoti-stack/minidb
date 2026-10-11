package storage

import (
	"bytes"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
)

// --- Phase 1: page header + checksums -------------------------------------

func TestPageHeaderRoundTrip(t *testing.T) {
	var p Page
	p.InitSlotted(PageTypeHeap)
	p.SetLSN(0xDEADBEEFCAFE)
	p.SetNextPage(42)
	p.SetPrevPage(InvalidPageID)
	p.SetExtra(7)

	if p.LSN() != 0xDEADBEEFCAFE {
		t.Fatalf("lsn = %#x", p.LSN())
	}
	if p.Type() != PageTypeHeap {
		t.Fatalf("type = %v", p.Type())
	}
	if p.NextPage() != 42 || p.PrevPage() != InvalidPageID || p.Extra() != 7 {
		t.Fatalf("links wrong: %v %v %v", p.NextPage(), p.PrevPage(), p.Extra())
	}
}

func TestPageChecksumDetectsCorruption(t *testing.T) {
	var p Page
	p.InitSlotted(PageTypeHeap)
	if _, err := p.Insert([]byte("hello world")); err != nil {
		t.Fatal(err)
	}
	p.StampChecksum()
	if err := p.VerifyChecksum(); err != nil {
		t.Fatalf("fresh page should verify: %v", err)
	}
	p.Data[PageSize-3] ^= 0x01 // simulate bit rot inside a record
	if err := p.VerifyChecksum(); err == nil {
		t.Fatal("corruption not detected")
	}
}

func TestEncodeDecodeFields(t *testing.T) {
	in := [][]byte{[]byte("alice"), {}, []byte("30"), bytes.Repeat([]byte("x"), 300)}
	rec, err := EncodeFields(in)
	if err != nil {
		t.Fatal(err)
	}
	out, err := DecodeFields(rec)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != len(in) {
		t.Fatalf("got %d fields want %d", len(out), len(in))
	}
	for i := range in {
		if !bytes.Equal(in[i], out[i]) {
			t.Fatalf("field %d: %q != %q", i, out[i], in[i])
		}
	}
	if _, err := DecodeFields(Record{1}); err == nil {
		t.Fatal("expected error on truncated record")
	}
}

func TestRIDEncoding(t *testing.T) {
	r := RID{PageID: 1234, Slot: 7}
	var b [RIDSize]byte
	EncodeRID(b[:], r)
	if got := DecodeRID(b[:]); got != r {
		t.Fatalf("got %v want %v", got, r)
	}
}

// --- Phase 2: slotted page -------------------------------------------------

func TestSlottedInsertGetDelete(t *testing.T) {
	var p Page
	p.InitSlotted(PageTypeHeap)

	recs := [][]byte{[]byte("a"), []byte("bb"), []byte("ccc"), {}}
	var slots []uint16
	for _, r := range recs {
		s, err := p.Insert(r)
		if err != nil {
			t.Fatal(err)
		}
		slots = append(slots, s)
	}
	for i, s := range slots {
		got, err := p.Get(s)
		if err != nil {
			t.Fatalf("slot %d: %v", s, err)
		}
		if !bytes.Equal(got, recs[i]) {
			t.Fatalf("slot %d: %q != %q", s, got, recs[i])
		}
	}
	if err := p.Delete(slots[1]); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Get(slots[1]); err != ErrRecordDeleted {
		t.Fatalf("want ErrRecordDeleted got %v", err)
	}
	if err := p.Delete(slots[1]); err != ErrRecordDeleted {
		t.Fatalf("double delete: %v", err)
	}
	// The other RIDs must be unaffected by the delete.
	if got, _ := p.Get(slots[2]); !bytes.Equal(got, recs[2]) {
		t.Fatal("delete disturbed a neighbouring record")
	}
	if _, err := p.Get(99); err != ErrInvalidSlot {
		t.Fatalf("want ErrInvalidSlot got %v", err)
	}
	if err := p.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestSlottedSlotReuse(t *testing.T) {
	var p Page
	p.InitSlotted(PageTypeHeap)
	s0, _ := p.Insert([]byte("first"))
	if err := p.Delete(s0); err != nil {
		t.Fatal(err)
	}
	s1, err := p.Insert([]byte("second"))
	if err != nil {
		t.Fatal(err)
	}
	if s1 != s0 {
		t.Fatalf("tombstoned slot not reused: %d vs %d", s1, s0)
	}
}

func TestSlottedUpdate(t *testing.T) {
	var p Page
	p.InitSlotted(PageTypeHeap)
	s, _ := p.Insert([]byte("medium size"))
	other, _ := p.Insert([]byte("neighbour"))

	if err := p.Update(s, []byte("tiny")); err != nil { // shrink, in place
		t.Fatal(err)
	}
	if got, _ := p.Get(s); string(got) != "tiny" {
		t.Fatalf("shrink: %q", got)
	}
	big := bytes.Repeat([]byte("z"), 1000)
	if err := p.Update(s, big); err != nil { // grow, relocates
		t.Fatal(err)
	}
	if got, _ := p.Get(s); !bytes.Equal(got, big) {
		t.Fatal("grow lost data")
	}
	if got, _ := p.Get(other); string(got) != "neighbour" {
		t.Fatalf("update disturbed neighbour: %q", got)
	}
	if err := p.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestSlottedPageFull(t *testing.T) {
	var p Page
	p.InitSlotted(PageTypeHeap)
	rec := bytes.Repeat([]byte("x"), 500)
	n := 0
	for {
		if _, err := p.Insert(rec); err != nil {
			if err != ErrPageFull {
				t.Fatalf("unexpected error: %v", err)
			}
			break
		}
		n++
		if n > 100 {
			t.Fatal("page never filled up")
		}
	}
	if n < 7 {
		t.Fatalf("only fit %d records of 500 bytes in a %d byte page", n, PageSize)
	}
	if _, err := p.Insert(bytes.Repeat([]byte("y"), MaxRecordSize+1)); err != ErrRecordTooLarge {
		t.Fatalf("want ErrRecordTooLarge got %v", err)
	}
}

func TestSlottedCompaction(t *testing.T) {
	var p Page
	p.InitSlotted(PageTypeHeap)
	// Fill the page, then delete every other record; the freed bytes must be
	// reclaimable so that inserts succeed again.
	rec := bytes.Repeat([]byte("q"), 400)
	var slots []uint16
	for {
		s, err := p.Insert(rec)
		if err != nil {
			break
		}
		slots = append(slots, s)
	}
	for i := 0; i < len(slots); i += 2 {
		if err := p.Delete(slots[i]); err != nil {
			t.Fatal(err)
		}
	}
	if p.DeadSpace() == 0 {
		t.Fatal("expected dead space after deletes")
	}
	before := map[uint16][]byte{}
	for i := 1; i < len(slots); i += 2 {
		r, err := p.Get(slots[i])
		if err != nil {
			t.Fatal(err)
		}
		before[slots[i]] = append([]byte(nil), r...)
	}
	p.Compact()
	if p.DeadSpace() != 0 {
		t.Fatalf("compaction left %d dead bytes", p.DeadSpace())
	}
	for s, want := range before {
		got, err := p.Get(s)
		if err != nil {
			t.Fatalf("slot %d after compaction: %v", s, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("slot %d changed across compaction", s)
		}
	}
	if _, err := p.Insert(rec); err != nil {
		t.Fatalf("insert after compaction: %v", err)
	}
	if err := p.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestSlottedInsertAtForRedo(t *testing.T) {
	var p Page
	p.InitSlotted(PageTypeHeap)
	if err := p.InsertAt(5, []byte("recovered")); err != nil {
		t.Fatal(err)
	}
	if p.NumSlots() != 6 {
		t.Fatalf("numSlots = %d", p.NumSlots())
	}
	for i := uint16(0); i < 5; i++ {
		if p.SlotOccupied(i) {
			t.Fatalf("slot %d should be a tombstone", i)
		}
	}
	if got, _ := p.Get(5); string(got) != "recovered" {
		t.Fatalf("got %q", got)
	}
}

func TestSlottedRandomizedAgainstModel(t *testing.T) {
	// Differential test: the page must behave exactly like a map[slot][]byte.
	var p Page
	p.InitSlotted(PageTypeHeap)
	model := map[uint16][]byte{}
	rng := rand.New(rand.NewSource(7))

	for i := 0; i < 20000; i++ {
		switch rng.Intn(4) {
		case 0, 1: // insert
			rec := make([]byte, rng.Intn(200))
			rng.Read(rec)
			s, err := p.Insert(rec)
			if err == ErrPageFull {
				continue
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, exists := model[s]; exists {
				t.Fatalf("slot %d handed out twice", s)
			}
			model[s] = rec
		case 2: // delete
			for s := range model {
				if err := p.Delete(s); err != nil {
					t.Fatal(err)
				}
				delete(model, s)
				break
			}
		case 3: // update
			for s := range model {
				rec := make([]byte, rng.Intn(300))
				rng.Read(rec)
				if err := p.Update(s, rec); err == nil {
					model[s] = rec
				} else if err != ErrPageFull {
					t.Fatal(err)
				}
				break
			}
		}
		if err := p.Validate(); err != nil {
			t.Fatalf("iteration %d: %v", i, err)
		}
	}
	for s, want := range model {
		got, err := p.Get(s)
		if err != nil {
			t.Fatalf("slot %d: %v", s, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("slot %d mismatch", s)
		}
	}
}

// --- Phase 1: disk manager -------------------------------------------------

func tempDB(t testing.TB) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "test.db")
}

func TestDiskManagerReadWrite(t *testing.T) {
	path := tempDB(t)
	dm, err := OpenDiskManager(path)
	if err != nil {
		t.Fatal(err)
	}
	if dm.NumPages() != 1 {
		t.Fatalf("fresh db should have just the meta page, got %d", dm.NumPages())
	}

	id, err := dm.AllocatePage()
	if err != nil {
		t.Fatal(err)
	}
	if id != 1 {
		t.Fatalf("first allocation = %v", id)
	}
	var p Page
	p.InitSlotted(PageTypeHeap)
	if _, err := p.Insert([]byte("durable")); err != nil {
		t.Fatal(err)
	}
	if err := dm.WritePage(id, &p); err != nil {
		t.Fatal(err)
	}
	if err := dm.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen: the data and the page count must survive.
	dm2, err := OpenDiskManager(path)
	if err != nil {
		t.Fatal(err)
	}
	defer dm2.Close()
	if dm2.NumPages() != 2 {
		t.Fatalf("numPages = %d", dm2.NumPages())
	}
	var q Page
	if err := dm2.ReadPage(id, &q); err != nil {
		t.Fatal(err)
	}
	got, err := q.Get(0)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "durable" {
		t.Fatalf("got %q", got)
	}
	if err := dm2.ReadPage(999, &q); err == nil {
		t.Fatal("expected error reading out of range page")
	}
}

func TestDiskManagerFreeListReuse(t *testing.T) {
	path := tempDB(t)
	dm, err := OpenDiskManager(path)
	if err != nil {
		t.Fatal(err)
	}
	var ids []PageID
	for i := 0; i < 5; i++ {
		id, err := dm.AllocatePage()
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	if err := dm.DeallocatePage(ids[2]); err != nil {
		t.Fatal(err)
	}
	if dm.FreeCount() != 1 {
		t.Fatalf("freeCount = %d", dm.FreeCount())
	}
	reused, err := dm.AllocatePage()
	if err != nil {
		t.Fatal(err)
	}
	if reused != ids[2] {
		t.Fatalf("expected free page %v to be reused, got %v", ids[2], reused)
	}
	if dm.NumPages() != 6 {
		t.Fatalf("file grew unnecessarily: %d pages", dm.NumPages())
	}
	if err := dm.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestDiskManagerDetectsCorruptPage(t *testing.T) {
	path := tempDB(t)
	dm, err := OpenDiskManager(path)
	if err != nil {
		t.Fatal(err)
	}
	id, _ := dm.AllocatePage()
	var p Page
	p.InitSlotted(PageTypeHeap)
	p.Insert([]byte("important"))
	if err := dm.WritePage(id, &p); err != nil {
		t.Fatal(err)
	}
	dm.Close()

	// Flip a bit on disk behind the engine's back.
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	var b [1]byte
	if _, err := f.ReadAt(b[:], int64(id)*PageSize+PageSize-5); err != nil {
		t.Fatal(err)
	}
	b[0] ^= 0xFF
	if _, err := f.WriteAt(b[:], int64(id)*PageSize+PageSize-5); err != nil {
		t.Fatal(err)
	}
	f.Close()

	dm2, err := OpenDiskManager(path)
	if err != nil {
		t.Fatal(err)
	}
	defer dm2.Close()
	var q Page
	if err := dm2.ReadPage(id, &q); err == nil {
		t.Fatal("corrupt page was accepted")
	}
	if dm2.Stats().Corruption == 0 {
		t.Fatal("corruption not counted")
	}
}

func TestDiskManagerTruncatesPartialTrailingPage(t *testing.T) {
	path := tempDB(t)
	dm, err := OpenDiskManager(path)
	if err != nil {
		t.Fatal(err)
	}
	dm.AllocatePage()
	dm.Close()

	// Simulate a crash while the file was being extended.
	f, _ := os.OpenFile(path, os.O_RDWR, 0o644)
	f.WriteAt([]byte("half a page"), 2*PageSize)
	f.Close()

	dm2, err := OpenDiskManager(path)
	if err != nil {
		t.Fatalf("open after torn extension: %v", err)
	}
	defer dm2.Close()
	if dm2.NumPages() != 2 {
		t.Fatalf("numPages = %d, partial page not dropped", dm2.NumPages())
	}
}

func BenchmarkDiskWritePage(b *testing.B) {
	dm, err := OpenDiskManager(tempDB(b))
	if err != nil {
		b.Fatal(err)
	}
	defer dm.Close()
	id, _ := dm.AllocatePage()
	var p Page
	p.InitSlotted(PageTypeHeap)
	p.Insert(bytes.Repeat([]byte("x"), 100))
	b.SetBytes(PageSize)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := dm.WritePage(id, &p); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSlottedInsert(b *testing.B) {
	rec := bytes.Repeat([]byte("x"), 64)
	var p Page
	p.InitSlotted(PageTypeHeap)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := p.Insert(rec); err == ErrPageFull {
			p.InitSlotted(PageTypeHeap)
		}
	}
}
