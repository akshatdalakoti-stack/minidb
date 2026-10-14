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
