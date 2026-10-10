package storage

import (
	"bytes"
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
