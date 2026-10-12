package wal

import (
	"bytes"
	"fmt"
	"path/filepath"
	"testing"

	"minidb/storage"
)

func tempLog(t testing.TB) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "test.wal")
}

func TestRecordRoundTrip(t *testing.T) {
	in := &Record{
		LSN:     16,
		TxnID:   9,
		PrevLSN: 3,
		Type:    TypeUpdate,
		PageID:  77,
		Slot:    5,
		Before:  []byte("old value"),
		After:   []byte("new value"),
	}
	buf, err := in.AppendTo(nil)
	if err != nil {
		t.Fatal(err)
	}
	out, n, err := DecodeRecord(buf)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(buf) {
		t.Fatalf("consumed %d of %d", n, len(buf))
	}
	if out.TxnID != in.TxnID || out.Type != in.Type || out.PageID != in.PageID || out.Slot != in.Slot {
		t.Fatalf("header mismatch: %v", out)
	}
	if !bytes.Equal(out.Before, in.Before) || !bytes.Equal(out.After, in.After) {
		t.Fatal("images mismatch")
	}
}

func TestRecordDetectsCorruption(t *testing.T) {
	r := &Record{Type: TypeInsert, PageID: 1, After: []byte("data")}
	buf, _ := r.AppendTo(nil)
	buf[10] ^= 0xFF
	if _, _, err := DecodeRecord(buf); err != ErrTornRecord {
		t.Fatalf("want ErrTornRecord got %v", err)
	}
	if _, _, err := DecodeRecord(buf[:len(buf)-3]); err != ErrTornRecord {
		t.Fatalf("truncated record: want ErrTornRecord got %v", err)
	}
}

func TestManagerAppendScanRead(t *testing.T) {
	path := tempLog(t)
	m, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	var lsns []LSN
	for i := 0; i < 100; i++ {
		lsn, err := m.Append(&Record{
			TxnID:  uint64(i%3 + 1),
			Type:   TypeInsert,
			PageID: storage.PageID(i),
			Slot:   uint16(i),
			After:  []byte(fmt.Sprintf("record-%d", i)),
		})
		if err != nil {
			t.Fatal(err)
		}
		lsns = append(lsns, lsn)
	}
	// LSNs must be strictly increasing (they are file offsets).
	for i := 1; i < len(lsns); i++ {
		if lsns[i] <= lsns[i-1] {
			t.Fatalf("LSNs not increasing: %d then %d", lsns[i-1], lsns[i])
		}
	}
	// Random access by LSN.
	r, err := m.Read(lsns[42])
	if err != nil {
		t.Fatal(err)
	}
	if string(r.After) != "record-42" {
		t.Fatalf("read gave %q", r.After)
	}
	// Sequential scan.
	n := 0
	if err := m.Scan(0, func(rec *Record) error {
		if string(rec.After) != fmt.Sprintf("record-%d", n) {
			return fmt.Errorf("record %d out of order: %q", n, rec.After)
		}
		n++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if n != 100 {
		t.Fatalf("scanned %d records", n)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen: the log must continue where it left off.
	m2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer m2.Close()
	if m2.NextLSN() <= lsns[99] {
		t.Fatalf("reopened log rewinds: next=%d last=%d", m2.NextLSN(), lsns[99])
	}
	count := 0
	m2.Scan(0, func(*Record) error { count++; return nil })
	if count != 100 {
		t.Fatalf("after reopen scanned %d", count)
	}
}

func TestManagerFlushSemantics(t *testing.T) {
	m, err := Open(tempLog(t))
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	lsn, _ := m.Append(&Record{Type: TypeCommit, TxnID: 1})
	if m.FlushedLSN() > lsn {
		t.Fatal("record reported durable before any flush")
	}
	if err := m.Flush(lsn + 1); err != nil {
		t.Fatal(err)
	}
	if m.FlushedLSN() <= lsn {
		t.Fatalf("flush did not advance: flushed=%d lsn=%d", m.FlushedLSN(), lsn)
	}
	before := m.Stats().Syncs
	if err := m.Flush(lsn); err != nil { // already durable
		t.Fatal(err)
	}
	if m.Stats().Syncs != before {
		t.Fatal("redundant fsync issued for an already durable LSN")
	}
}
