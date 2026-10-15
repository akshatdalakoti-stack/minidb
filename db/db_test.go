package db

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"minidb/storage"
	"minidb/txn"
)

func testOptions() Options {
	o := DefaultOptions()
	o.PoolSize = 64
	o.LockTimeout = 3 * time.Second
	return o
}

func openTestDB(t testing.TB, dir string) *DB {
	t.Helper()
	d, err := Open(dir, testOptions())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	return d
}

func usersSchema() Schema {
	return Schema{Columns: []Column{
		{Name: "id", Type: TypeInt},
		{Name: "name", Type: TypeText},
		{Name: "age", Type: TypeInt},
	}}
}

func userRow(i int) Row {
	return Row{IntValue(int64(i)), TextValue(fmt.Sprintf("user-%04d", i)), IntValue(int64(20 + i%50))}
}

func TestCreateTableAndRoundTrip(t *testing.T) {
	d := openTestDB(t, t.TempDir())
	defer d.Close()

	tb, err := d.CreateTable("users", usersSchema())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := d.CreateTable("users", usersSchema()); err == nil {
		t.Fatal("duplicate table name accepted")
	}

	var rid storage.RID
	err = d.Update(func(tx *txn.Txn) error {
		var err error
		rid, err = d.Insert(tx, tb, userRow(1))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	err = d.View(func(tx *txn.Txn) error {
		row, err := d.Get(tx, tb, rid)
		if err != nil {
			return err
		}
		if row[0].Int != 1 || row[1].Text != "user-0001" {
			return fmt.Errorf("got %v", row)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestValueEncodingIsOrderPreserving(t *testing.T) {
	// The B+tree compares keys as bytes, so the integer encoding has to sort
	// the same way integers do - including across zero.
	vals := []int64{-1 << 62, -1000, -1, 0, 1, 1000, 1 << 62}
	for i := 1; i < len(vals); i++ {
		a, b := IndexKey(IntValue(vals[i-1])), IndexKey(IntValue(vals[i]))
		if string(a) >= string(b) {
			t.Fatalf("encoding not order preserving: %d vs %d", vals[i-1], vals[i])
		}
	}
	for _, v := range vals {
		if got := DecodeInt(EncodeInt(v)); got != v {
			t.Fatalf("round trip: %d -> %d", v, got)
		}
	}
}

func TestRowEncodingRoundTrip(t *testing.T) {
	s := usersSchema()
	rows := []Row{
		{IntValue(7), TextValue("bob"), IntValue(-3)},
		{IntValue(0), TextValue(""), NullValue(TypeInt)},
		{NullValue(TypeInt), TextValue("x"), IntValue(1 << 40)},
	}
	for _, want := range rows {
		rec, err := want.Encode(s)
		if err != nil {
			t.Fatal(err)
		}
		got, err := DecodeRow(s, rec)
		if err != nil {
			t.Fatal(err)
		}
		for i := range want {
			if got[i].Null != want[i].Null || got[i].Int != want[i].Int || got[i].Text != want[i].Text {
				t.Fatalf("column %d: got %+v want %+v", i, got[i], want[i])
			}
		}
	}
}

func TestUpdateDeleteAndScan(t *testing.T) {
	d := openTestDB(t, t.TempDir())
	defer d.Close()
	tb, _ := d.CreateTable("users", usersSchema())

	rids := make([]storage.RID, 0, 200)
	if err := d.Update(func(tx *txn.Txn) error {
		for i := 0; i < 200; i++ {
			rid, err := d.Insert(tx, tb, userRow(i))
			if err != nil {
				return err
			}
			rids = append(rids, rid)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := d.Update(func(tx *txn.Txn) error {
		row := userRow(0)
		row[1] = TextValue("renamed")
		_, err := d.UpdateRow(tx, tb, rids[0], row)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := d.Update(func(tx *txn.Txn) error {
		return d.DeleteRow(tx, tb, rids[1])
	}); err != nil {
		t.Fatal(err)
	}

	seen := 0
	renamed := false
	if err := d.View(func(tx *txn.Txn) error {
		return d.Scan(tx, tb, func(r ScanRow) error {
			seen++
			if r.Row[1].Text == "renamed" {
				renamed = true
			}
			if r.Row[0].Int == 1 {
				return fmt.Errorf("deleted row still visible")
			}
			return nil
		})
	}); err != nil {
		t.Fatal(err)
	}
	if seen != 199 {
		t.Fatalf("scan saw %d rows, want 199", seen)
	}
	if !renamed {
		t.Fatal("update not visible in scan")
	}
}

func TestIndexLookupAndRange(t *testing.T) {
	d := openTestDB(t, t.TempDir())
	defer d.Close()
	tb, _ := d.CreateTable("users", usersSchema())

	if err := d.Update(func(tx *txn.Txn) error {
		for i := 0; i < 1000; i++ {
			if _, err := d.Insert(tx, tb, userRow(i)); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	// Index built after the data exists: it must be back-filled.
	tb, err := d.CreateIndex("users", "users_by_id", "id", true)
	if err != nil {
		t.Fatal(err)
	}
	if err := d.View(func(tx *txn.Txn) error {
		rows, err := d.IndexLookup(tx, tb, "id", IntValue(500))
		if err != nil {
			return err
		}
		if len(rows) != 1 || rows[0].Row[1].Text != "user-0500" {
			return fmt.Errorf("lookup gave %v", rows)
		}
		count := 0
		err = d.IndexRange(tx, tb, "id", IntValue(100), IntValue(199), func(r ScanRow) error {
			count++
			if r.Row[0].Int < 100 || r.Row[0].Int > 199 {
				return fmt.Errorf("range scan returned %v", r.Row[0])
			}
			return nil
		})
		if err != nil {
			return err
		}
		if count != 100 {
			return fmt.Errorf("range scan returned %d rows, want 100", count)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	// Rows inserted after the index exists must show up in it, and deleted
	// rows must disappear from it.
	if err := d.Update(func(tx *txn.Txn) error {
		_, err := d.Insert(tx, tb, Row{IntValue(5000), TextValue("late"), IntValue(1)})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := d.View(func(tx *txn.Txn) error {
		rows, err := d.IndexLookup(tx, tb, "id", IntValue(5000))
		if err != nil {
			return err
		}
		if len(rows) != 1 {
			return fmt.Errorf("new row not indexed: %v", rows)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestAbortRollsBackEverything(t *testing.T) {
	d := openTestDB(t, t.TempDir())
	defer d.Close()
	tb, _ := d.CreateTable("users", usersSchema())
	tb, _ = d.CreateIndex("users", "users_by_id", "id", false)

	// A committed baseline.
	if err := d.Update(func(tx *txn.Txn) error {
		for i := 0; i < 50; i++ {
			if _, err := d.Insert(tx, tb, userRow(i)); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	before, _ := d.Count(tb)

	// Now a transaction that does a lot and then changes its mind.
	tx, err := d.Begin()
	if err != nil {
		t.Fatal(err)
	}
	var doomed []storage.RID
	for i := 100; i < 300; i++ {
		rid, err := d.Insert(tx, tb, userRow(i))
		if err != nil {
			t.Fatal(err)
		}
		doomed = append(doomed, rid)
	}
	if err := d.Abort(tx); err != nil {
		t.Fatal(err)
	}

	after, _ := d.Count(tb)
	if after != before {
		t.Fatalf("rollback left %d rows behind (%d -> %d)", after-before, before, after)
	}
	if err := d.View(func(tx *txn.Txn) error {
		for _, rid := range doomed {
			if _, err := d.Get(tx, tb, rid); err == nil {
				return fmt.Errorf("aborted row %v is still readable", rid)
			}
		}
		// The index must have been rolled back too, not just the heap.
		rows, err := d.IndexLookup(tx, tb, "id", IntValue(150))
		if err != nil {
			return err
		}
		if len(rows) != 0 {
			return fmt.Errorf("aborted row still in the index: %v", rows)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestAbortOfUpdateAndDelete(t *testing.T) {
	d := openTestDB(t, t.TempDir())
	defer d.Close()
	tb, _ := d.CreateTable("users", usersSchema())

	var rids []storage.RID
	if err := d.Update(func(tx *txn.Txn) error {
		for i := 0; i < 10; i++ {
			rid, err := d.Insert(tx, tb, userRow(i))
			if err != nil {
				return err
			}
			rids = append(rids, rid)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	tx, _ := d.Begin()
	row := userRow(0)
	row[1] = TextValue("changed-my-mind")
	if _, err := d.UpdateRow(tx, tb, rids[0], row); err != nil {
		t.Fatal(err)
	}
	if err := d.DeleteRow(tx, tb, rids[1]); err != nil {
		t.Fatal(err)
	}
	if err := d.Abort(tx); err != nil {
		t.Fatal(err)
	}

	if err := d.View(func(tx *txn.Txn) error {
		r0, err := d.Get(tx, tb, rids[0])
		if err != nil {
			return err
		}
		if r0[1].Text != "user-0000" {
			return fmt.Errorf("update not rolled back: %v", r0[1])
		}
		r1, err := d.Get(tx, tb, rids[1])
		if err != nil {
			return fmt.Errorf("deleted row not restored: %w", err)
		}
		if r1[0].Int != 1 {
			return fmt.Errorf("restored row is wrong: %v", r1)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestIsolationUncommittedRowsAreNotVisible(t *testing.T) {
	d := openTestDB(t, t.TempDir())
	defer d.Close()
	tb, _ := d.CreateTable("users", usersSchema())

	writer, _ := d.Begin()
	rid, err := d.Insert(writer, tb, userRow(1))
	if err != nil {
		t.Fatal(err)
	}

	// A reader that tries to read the uncommitted row must block on the X lock
	// rather than see it.
	done := make(chan error, 1)
	go func() {
		reader, _ := d.Begin()
		_, err := d.Get(reader, tb, rid)
		d.Abort(reader)
		done <- err
	}()
	select {
	case err := <-done:
		t.Fatalf("reader saw an uncommitted row (err=%v)", err)
	case <-time.After(100 * time.Millisecond):
	}
	if err := d.Commit(writer); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("reader failed after the writer committed: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("reader never woke up")
	}
}

func TestPersistenceAcrossCleanRestart(t *testing.T) {
	dir := t.TempDir()
	d := openTestDB(t, dir)
	tb, _ := d.CreateTable("users", usersSchema())
	d.CreateIndex("users", "users_by_id", "id", false)
	tb, _ = d.Table("users")
	if err := d.Update(func(tx *txn.Txn) error {
		for i := 0; i < 500; i++ {
			if _, err := d.Insert(tx, tb, userRow(i)); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}

	d2 := openTestDB(t, dir)
	defer d2.Close()
	tb2, err := d2.Table("users")
	if err != nil {
		t.Fatalf("table definition did not survive: %v", err)
	}
	if tb2.Def.Schema.String() != usersSchema().String() {
		t.Fatalf("schema changed: %v", tb2.Def.Schema)
	}
	n, _ := d2.Count(tb2)
	if n != 500 {
		t.Fatalf("%d rows after restart, want 500", n)
	}
	if err := d2.View(func(tx *txn.Txn) error {
		rows, err := d2.IndexLookup(tx, tb2, "id", IntValue(499))
		if err != nil {
			return err
		}
		if len(rows) != 1 {
			return fmt.Errorf("index did not survive restart: %v", rows)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// --- Phase 8: crash recovery ----------------------------------------------

func TestRecoveryRedoesCommittedWork(t *testing.T) {
	dir := t.TempDir()
	d := openTestDB(t, dir)
	tb, _ := d.CreateTable("users", usersSchema())
	tb, _ = d.CreateIndex("users", "users_by_id", "id", false)

	if err := d.Update(func(tx *txn.Txn) error {
		for i := 0; i < 300; i++ {
			if _, err := d.Insert(tx, tb, userRow(i)); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	// The commit is durable in the log, but the pages are still dirty in RAM.
	// Pull the plug.
	d.SimulateCrash()

	d2 := openTestDB(t, dir)
	defer d2.Close()
	rep := d2.Report()
	t.Log(rep)
	if rep.RecordsRedone == 0 {
		t.Fatal("recovery redid nothing, so the crash test proved nothing")
	}
	tb2, err := d2.Table("users")
	if err != nil {
		t.Fatal(err)
	}
	n, _ := d2.Count(tb2)
	if n != 300 {
		t.Fatalf("%d rows after recovery, want 300", n)
	}
	if err := d2.View(func(tx *txn.Txn) error {
		for _, i := range []int64{0, 42, 299} {
			rows, err := d2.IndexLookup(tx, tb2, "id", IntValue(i))
			if err != nil {
				return err
			}
			if len(rows) != 1 {
				return fmt.Errorf("id %d: index has %d entries after recovery", i, len(rows))
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestRecoveryUndoesUncommittedWork(t *testing.T) {
	dir := t.TempDir()
	d := openTestDB(t, dir)
	tb, _ := d.CreateTable("users", usersSchema())
	tb, _ = d.CreateIndex("users", "users_by_id", "id", false)

	// Committed work.
	if err := d.Update(func(tx *txn.Txn) error {
		for i := 0; i < 100; i++ {
			if _, err := d.Insert(tx, tb, userRow(i)); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	// Uncommitted work from a transaction that will never finish.
	loser, _ := d.Begin()
	for i := 1000; i < 1200; i++ {
		if _, err := d.Insert(loser, tb, userRow(i)); err != nil {
			t.Fatal(err)
		}
	}
	// Force its pages to disk, so recovery genuinely has to undo them rather
	// than just "not redo" them.
	if err := d.Pool().FlushAllPages(); err != nil {
		t.Fatal(err)
	}
	d.SimulateCrash()

	d2 := openTestDB(t, dir)
	defer d2.Close()
	rep := d2.Report()
	t.Log(rep)
	if len(rep.Losers) != 1 || rep.Losers[0] != loser.ID() {
		t.Fatalf("expected txn %d to be the loser, report says %v", loser.ID(), rep.Losers)
	}
	if rep.RecordsUndone == 0 {
		t.Fatal("recovery undid nothing")
	}
	tb2, _ := d2.Table("users")
	n, _ := d2.Count(tb2)
	if n != 100 {
		t.Fatalf("%d rows after recovery, want 100 (the uncommitted 200 must be gone)", n)
	}
	if err := d2.View(func(tx *txn.Txn) error {
		rows, err := d2.IndexLookup(tx, tb2, "id", IntValue(1100))
		if err != nil {
			return err
		}
		if len(rows) != 0 {
			return fmt.Errorf("uncommitted row survived in the index: %v", rows)
		}
		rows, err = d2.IndexLookup(tx, tb2, "id", IntValue(50))
		if err != nil {
			return err
		}
		if len(rows) != 1 {
			return fmt.Errorf("committed row lost: %v", rows)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestRecoveryIsIdempotent(t *testing.T) {
	// Crash, recover, crash again during the next run, recover again: the
	// answer must be the same every time.
	dir := t.TempDir()
	d := openTestDB(t, dir)
	tb, _ := d.CreateTable("users", usersSchema())
	d.Update(func(tx *txn.Txn) error {
		for i := 0; i < 200; i++ {
			if _, err := d.Insert(tx, tb, userRow(i)); err != nil {
				return err
			}
		}
		return nil
	})
	loser, _ := d.Begin()
	d.Insert(loser, tb, userRow(9999))
	d.Pool().FlushAllPages()
	d.SimulateCrash()

	for round := 0; round < 3; round++ {
		d2 := openTestDB(t, dir)
		tb2, _ := d2.Table("users")
		n, _ := d2.Count(tb2)
		if n != 200 {
			t.Fatalf("round %d: %d rows, want 200", round, n)
		}
		if round < 2 {
			d2.SimulateCrash()
		} else {
			d2.Close()
		}
	}
}

func TestRecoveryAfterManyTransactions(t *testing.T) {
	dir := t.TempDir()
	d := openTestDB(t, dir)
	tb, _ := d.CreateTable("users", usersSchema())
	tb, _ = d.CreateIndex("users", "users_by_id", "id", false)

	committed := 0
	for i := 0; i < 200; i++ {
		tx, _ := d.Begin()
		if _, err := d.Insert(tx, tb, userRow(i)); err != nil {
			t.Fatal(err)
		}
		if i%3 == 0 {
			// Every third transaction rolls back.
			if err := d.Abort(tx); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if err := d.Commit(tx); err != nil {
			t.Fatal(err)
		}
		committed++
	}
	d.SimulateCrash()

	d2 := openTestDB(t, dir)
	defer d2.Close()
	tb2, _ := d2.Table("users")
	n, _ := d2.Count(tb2)
	if n != committed {
		t.Fatalf("%d rows after recovery, want %d", n, committed)
	}
	// Index and heap must agree.
	if err := d2.View(func(tx *txn.Txn) error {
		return d2.Scan(tx, tb2, func(r ScanRow) error {
			rows, err := d2.IndexLookup(tx, tb2, "id", r.Row[0])
			if err != nil {
				return err
			}
			if len(rows) != 1 {
				return fmt.Errorf("id %v: heap has the row, index has %d entries", r.Row[0], len(rows))
			}
			return nil
		})
	}); err != nil {
		t.Fatal(err)
	}
}

func TestCheckpointShortensRecovery(t *testing.T) {
	dir := t.TempDir()
	d := openTestDB(t, dir)
	tb, _ := d.CreateTable("users", usersSchema())
	d.Update(func(tx *txn.Txn) error {
		for i := 0; i < 500; i++ {
			if _, err := d.Insert(tx, tb, userRow(i)); err != nil {
				return err
			}
		}
		return nil
	})
	if err := d.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	// Only this work is after the checkpoint.
	d.Update(func(tx *txn.Txn) error {
		_, err := d.Insert(tx, tb, userRow(1000))
		return err
	})
	d.SimulateCrash()

	d2 := openTestDB(t, dir)
	defer d2.Close()
	rep := d2.Report()
	t.Log(rep)
	if rep.StartLSN == 0 {
		t.Fatal("recovery started from the beginning of the log despite the checkpoint")
	}
	if rep.RecordsScanned > 200 {
		t.Fatalf("recovery scanned %d records; the checkpoint should have cut that down", rep.RecordsScanned)
	}
	tb2, _ := d2.Table("users")
	if n, _ := d2.Count(tb2); n != 501 {
		t.Fatalf("%d rows after recovery, want 501", n)
	}
}

// --- Phase 10: concurrency -------------------------------------------------

func TestConcurrentTransactions(t *testing.T) {
	d := openTestDB(t, t.TempDir())
	defer d.Close()
	tb, _ := d.CreateTable("users", usersSchema())
	tb, _ = d.CreateIndex("users", "users_by_id", "id", false)

	const goroutines = 20
	const perGoroutine = 50
	var wg sync.WaitGroup
	var mu sync.Mutex
	inserted := 0
	failures := 0

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				id := g*perGoroutine + i
				err := d.Update(func(tx *txn.Txn) error {
					_, err := d.Insert(tx, tb, userRow(id))
					return err
				})
				mu.Lock()
				if err != nil {
					failures++
				} else {
					inserted++
				}
				mu.Unlock()
			}
		}(g)
	}
	wg.Wait()
	if failures != 0 {
		t.Fatalf("%d transactions failed", failures)
	}
	n, err := d.Count(tb)
	if err != nil {
		t.Fatal(err)
	}
	if n != inserted {
		t.Fatalf("heap holds %d rows, %d were inserted", n, inserted)
	}
	// Every row must be findable through the index too.
	if err := d.View(func(tx *txn.Txn) error {
		for id := 0; id < goroutines*perGoroutine; id += 37 {
			rows, err := d.IndexLookup(tx, tb, "id", IntValue(int64(id)))
			if err != nil {
				return err
			}
			if len(rows) != 1 {
				return fmt.Errorf("id %d has %d index entries", id, len(rows))
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if s := d.Pool().Stats(); s.Pinned != 0 {
		t.Fatalf("pins leaked: %v", s)
	}
	t.Log(d.Stats())
}

func TestConcurrentReadersAndWriters(t *testing.T) {
	d := openTestDB(t, t.TempDir())
	defer d.Close()
	tb, _ := d.CreateTable("users", usersSchema())

	var rids []storage.RID
	d.Update(func(tx *txn.Txn) error {
		for i := 0; i < 200; i++ {
			rid, err := d.Insert(tx, tb, userRow(i))
			if err != nil {
				return err
			}
			rids = append(rids, rid)
		}
		return nil
	})

	var wg sync.WaitGroup
	var mu sync.Mutex
	conflicts := 0
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 40; i++ {
				rid := rids[(g*13+i)%len(rids)]
				err := d.Update(func(tx *txn.Txn) error {
					row, err := d.Get(tx, tb, rid)
					if err != nil {
						return err
					}
					row[2] = IntValue(row[2].Int + 1)
					_, err = d.UpdateRow(tx, tb, rid, row)
					return err
				})
				if err != nil {
					// Deadlock or timeout: a legal outcome under 2PL, as long
					// as the transaction was rolled back cleanly.
					mu.Lock()
					conflicts++
					mu.Unlock()
				}
			}
		}(g)
	}
	wg.Wait()
	t.Logf("%d transactions had to be retried (deadlock/timeout)", conflicts)

	// Whatever happened, the database must still be consistent and readable.
	n, err := d.Count(tb)
	if err != nil {
		t.Fatal(err)
	}
	if n != 200 {
		t.Fatalf("row count changed to %d", n)
	}
	if err := d.View(func(tx *txn.Txn) error {
		return d.Scan(tx, tb, func(r ScanRow) error {
			if r.Row[2].Int < 20 {
				return fmt.Errorf("row %v has a nonsensical age", r.Row)
			}
			return nil
		})
	}); err != nil {
		t.Fatal(err)
	}
}

// TestHundredGoroutines is the concurrency exercise from the brief: 100
// goroutines inserting, reading and updating at the same time, followed by a
// consistency check of the whole database.
func TestHundredGoroutines(t *testing.T) {
	d := openTestDB(t, t.TempDir())
	defer d.Close()
	tb, _ := d.CreateTable("users", usersSchema())
	tb, _ = d.CreateIndex("users", "users_by_id", "id", false)

	// A committed baseline for the readers and updaters to work on.
	var baseRIDs []storage.RID
	if err := d.Update(func(tx *txn.Txn) error {
		for i := 0; i < 500; i++ {
			rid, err := d.Insert(tx, tb, userRow(i))
			if err != nil {
				return err
			}
			baseRIDs = append(baseRIDs, rid)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	const goroutines = 100
	var wg sync.WaitGroup
	var inserted, reads, updates, conflicts int64
	var mu sync.Mutex
	bump := func(p *int64) { mu.Lock(); *p++; mu.Unlock() }

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 20; i++ {
				switch g % 3 {
				case 0: // writers
					id := 10000 + g*100 + i
					if err := d.Update(func(tx *txn.Txn) error {
						_, err := d.Insert(tx, tb, userRow(id))
						return err
					}); err != nil {
						bump(&conflicts)
					} else {
						bump(&inserted)
					}
				case 1: // readers
					if err := d.View(func(tx *txn.Txn) error {
						_, err := d.IndexLookup(tx, tb, "id", IntValue(int64((g*7+i)%500)))
						return err
					}); err != nil {
						bump(&conflicts)
					} else {
						bump(&reads)
					}
				default: // updaters
					rid := baseRIDs[(g*13+i)%len(baseRIDs)]
					if err := d.Update(func(tx *txn.Txn) error {
						row, err := d.Get(tx, tb, rid)
						if err != nil {
							return err
						}
						row[2] = IntValue(row[2].Int + 1)
						_, err = d.UpdateRow(tx, tb, rid, row)
						return err
					}); err != nil {
						bump(&conflicts)
					} else {
						bump(&updates)
					}
				}
			}
		}(g)
	}
	wg.Wait()
	t.Logf("%d inserts, %d reads, %d updates, %d conflicts", inserted, reads, updates, conflicts)

	// Consistency: the heap holds exactly the rows that committed, and every
	// one of them is reachable through the index.
	n, err := d.Count(tb)
	if err != nil {
		t.Fatal(err)
	}
	if int64(n) != 500+inserted {
		t.Fatalf("heap holds %d rows, expected %d", n, 500+inserted)
	}
	if err := d.View(func(tx *txn.Txn) error {
		return d.Scan(tx, tb, func(r ScanRow) error {
			rows, err := d.IndexLookup(tx, tb, "id", r.Row[0])
			if err != nil {
				return err
			}
			for _, got := range rows {
				if got.RID == r.RID {
					return nil
				}
			}
			return fmt.Errorf("row %v (id %v) is missing from the index", r.RID, r.Row[0])
		})
	}); err != nil {
		t.Fatal(err)
	}
	if s := d.Pool().Stats(); s.Pinned != 0 {
		t.Fatalf("pins leaked: %v", s)
	}
}

// --- the "definition of done" walkthrough ---------------------------------

func TestEndToEndDefinitionOfDone(t *testing.T) {
	if testing.Short() {
		t.Skip("long end to end test")
	}
	dir := t.TempDir()
	opts := testOptions()
	opts.PoolSize = 256
	opts.NoSync = true // keep the test quick; the crash still drops the pool
	d, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}

	// Create table, insert 100k records.
	tb, err := d.CreateTable("users", usersSchema())
	if err != nil {
		t.Fatal(err)
	}
	const n = 100000
	start := time.Now()
	if err := d.Update(func(tx *txn.Txn) error {
		for i := 0; i < n; i++ {
			if _, err := d.Insert(tx, tb, userRow(i)); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	t.Logf("inserted %d rows in %s", n, time.Since(start))

	// Build the index afterwards, over existing data.
	start = time.Now()
	tb, err = d.CreateIndex("users", "users_by_id", "id", true)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("built the index in %s", time.Since(start))

	// Point lookups and range scans.
	if err := d.View(func(tx *txn.Txn) error {
		for _, id := range []int64{0, 1, 50000, n - 1} {
			rows, err := d.IndexLookup(tx, tb, "id", IntValue(id))
			if err != nil {
				return err
			}
			if len(rows) != 1 {
				return fmt.Errorf("point lookup %d returned %d rows", id, len(rows))
			}
		}
		count := 0
		if err := d.IndexRange(tx, tb, "id", IntValue(1000), IntValue(1999), func(ScanRow) error {
			count++
			return nil
		}); err != nil {
			return err
		}
		if count != 1000 {
			return fmt.Errorf("range scan returned %d rows", count)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	// Updates and deletes inside one committed transaction.
	var updatedRID storage.RID
	if err := d.Update(func(tx *txn.Txn) error {
		rows, err := d.IndexLookup(tx, tb, "id", IntValue(1234))
		if err != nil {
			return err
		}
		row := rows[0].Row
		row[1] = TextValue("updated-before-the-crash")
		updatedRID, err = d.UpdateRow(tx, tb, rows[0].RID, row)
		if err != nil {
			return err
		}
		del, err := d.IndexLookup(tx, tb, "id", IntValue(4321))
		if err != nil {
			return err
		}
		return d.DeleteRow(tx, tb, del[0].RID)
	}); err != nil {
		t.Fatal(err)
	}

	// An uncommitted transaction that the crash will eat.
	loser, _ := d.Begin()
	if _, err := d.Insert(loser, tb, Row{IntValue(-1), TextValue("never-committed"), IntValue(0)}); err != nil {
		t.Fatal(err)
	}

	// Crash the process.
	d.SimulateCrash()

	// Restart: recovery runs during Open.
	d2, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer d2.Close()
	t.Log(d2.Report())

	tb2, err := d2.Table("users")
	if err != nil {
		t.Fatal(err)
	}
	count, err := d2.Count(tb2)
	if err != nil {
		t.Fatal(err)
	}
	if count != n-1 {
		t.Fatalf("after recovery the table holds %d rows, want %d", count, n-1)
	}
	if err := d2.View(func(tx *txn.Txn) error {
		row, err := d2.Get(tx, tb2, updatedRID)
		if err != nil {
			return err
		}
		if row[1].Text != "updated-before-the-crash" {
			return fmt.Errorf("committed update was lost: %v", row[1])
		}
		if rows, err := d2.IndexLookup(tx, tb2, "id", IntValue(4321)); err != nil || len(rows) != 0 {
			return fmt.Errorf("committed delete was lost: %v %v", rows, err)
		}
		if rows, err := d2.IndexLookup(tx, tb2, "id", IntValue(-1)); err != nil || len(rows) != 0 {
			return fmt.Errorf("uncommitted insert survived: %v %v", rows, err)
		}
		if rows, err := d2.IndexLookup(tx, tb2, "id", IntValue(99999)); err != nil || len(rows) != 1 {
			return fmt.Errorf("committed row missing after recovery: %v %v", rows, err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	t.Log(d2.Stats())
}
