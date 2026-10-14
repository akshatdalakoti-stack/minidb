package db

import (
	"fmt"
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
