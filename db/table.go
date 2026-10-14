package db

import (
	"fmt"

	"minidb/storage"
	"minidb/txn"
)

// ---------------------------------------------------------------------------
// Row level operations: the layer where a transaction, a heap file and a set
// of indexes finally meet.
//
// Every operation follows the same shape:
//
//	1. take the right lock (2PL: it is held until commit)
//	2. change the heap
//	3. bring every index on the table back in step
//
// Step 3 is why the heap's Update reports a new RID instead of hiding a
// relocation: if a record moves, every index entry that points at it has to be
// rewritten, and only this layer knows what those indexes are.
// ---------------------------------------------------------------------------

// Insert adds a row to a table and updates its indexes.
func (d *DB) Insert(t *txn.Txn, tb *Table, row Row) (storage.RID, error) {
	if t == nil {
		return storage.InvalidRID, txn.ErrUnknownTxn
	}
	if t.Aborting() {
		return storage.InvalidRID, txn.ErrTxnAborted
	}
	rec, err := row.Encode(tb.Def.Schema)
	if err != nil {
		return storage.InvalidRID, err
	}
	ctx := d.ctx(t)
	rid, err := tb.Heap.Insert(ctx, rec)
	if err != nil {
		return storage.InvalidRID, err
	}
	// Lock the row we just created. Nobody else can see it yet (they would
	// have to find it first), but the lock has to exist so that a reader that
	// stumbles onto it during a scan waits for our commit.
	if err := d.txns.Locks().Acquire(t, txn.RowKey(tb.Def.ID, rid), txn.LockExclusive); err != nil {
		return storage.InvalidRID, err
	}
	for i, tree := range tb.Indexes {
		col := tb.Def.Schema.ColumnIndex(tb.Def.Indexes[i].Column)
		if col < 0 {
			continue
		}
		if err := tree.Insert(ctx, IndexKey(row[col]), rid); err != nil {
			return storage.InvalidRID, err
		}
	}
	return rid, nil
}

// Get reads one row by RID, taking a shared lock on it.
func (d *DB) Get(t *txn.Txn, tb *Table, rid storage.RID) (Row, error) {
	if err := d.txns.Locks().Acquire(t, txn.RowKey(tb.Def.ID, rid), txn.LockShared); err != nil {
		return nil, err
	}
	rec, err := tb.Heap.Get(rid)
	if err != nil {
		return nil, err
	}
	return DecodeRow(tb.Def.Schema, rec)
}

// UpdateRow replaces a row. It returns the row's RID, which changes if the new
// version did not fit on its old page.
func (d *DB) UpdateRow(t *txn.Txn, tb *Table, rid storage.RID, row Row) (storage.RID, error) {
	if t == nil {
		return storage.InvalidRID, txn.ErrUnknownTxn
	}
	if err := d.txns.Locks().Acquire(t, txn.RowKey(tb.Def.ID, rid), txn.LockExclusive); err != nil {
		return storage.InvalidRID, err
	}
	oldRec, err := tb.Heap.Get(rid)
	if err != nil {
		return storage.InvalidRID, err
	}
	oldRow, err := DecodeRow(tb.Def.Schema, oldRec)
	if err != nil {
		return storage.InvalidRID, err
	}
	newRec, err := row.Encode(tb.Def.Schema)
	if err != nil {
		return storage.InvalidRID, err
	}
	ctx := d.ctx(t)
	newRID, err := tb.Heap.Update(ctx, rid, newRec)
	if err != nil {
		return storage.InvalidRID, err
	}
	if newRID != rid {
		if err := d.txns.Locks().Acquire(t, txn.RowKey(tb.Def.ID, newRID), txn.LockExclusive); err != nil {
			return storage.InvalidRID, err
		}
	}
	for i, tree := range tb.Indexes {
		col := tb.Def.Schema.ColumnIndex(tb.Def.Indexes[i].Column)
		if col < 0 {
			continue
		}
		oldKey, newKey := IndexKey(oldRow[col]), IndexKey(row[col])
		if newRID == rid && string(oldKey) == string(newKey) {
			continue // neither the key nor the address changed
		}
		if _, err := tree.Delete(ctx, oldKey, rid); err != nil {
			return storage.InvalidRID, err
		}
		if err := tree.Insert(ctx, newKey, newRID); err != nil {
			return storage.InvalidRID, err
		}
	}
	return newRID, nil
}

// DeleteRow removes a row and its index entries.
func (d *DB) DeleteRow(t *txn.Txn, tb *Table, rid storage.RID) error {
	if t == nil {
		return txn.ErrUnknownTxn
	}
	if err := d.txns.Locks().Acquire(t, txn.RowKey(tb.Def.ID, rid), txn.LockExclusive); err != nil {
		return err
	}
	rec, err := tb.Heap.Get(rid)
	if err != nil {
		return err
	}
	row, err := DecodeRow(tb.Def.Schema, rec)
	if err != nil {
		return err
	}
	ctx := d.ctx(t)
	for i, tree := range tb.Indexes {
		col := tb.Def.Schema.ColumnIndex(tb.Def.Indexes[i].Column)
		if col < 0 {
			continue
		}
		if _, err := tree.Delete(ctx, IndexKey(row[col]), rid); err != nil {
			return err
		}
	}
	return tb.Heap.Delete(ctx, rid)
}

// ScanRow is one row produced by a scan.
type ScanRow struct {
	RID storage.RID
	Row Row
}

// Scan walks every row of a table, taking a shared lock on each one. fn may
// return ErrStopScan to finish early.
func (d *DB) Scan(t *txn.Txn, tb *Table, fn func(ScanRow) error) error {
	s := tb.Heap.Scan()
	for s.Next() {
		r := s.Row()
		if err := d.txns.Locks().Acquire(t, txn.RowKey(tb.Def.ID, r.RID), txn.LockShared); err != nil {
			return err
		}
		row, err := DecodeRow(tb.Def.Schema, r.Record)
		if err != nil {
			return err
		}
		if err := fn(ScanRow{RID: r.RID, Row: row}); err != nil {
			if err == ErrStopScan {
				return nil
			}
			return err
		}
	}
	return s.Err()
}

// ErrStopScan ends a scan early without reporting an error.
var ErrStopScan = fmt.Errorf("db: stop scan")

// IndexLookup finds the rows whose indexed column equals v. This is the point
// of having an index: it turns a full scan into a tree descent.
func (d *DB) IndexLookup(t *txn.Txn, tb *Table, column string, v Value) ([]ScanRow, error) {
	tree, _, ok := tb.IndexOn(column)
	if !ok {
		return nil, fmt.Errorf("%w: no index on %s.%s", ErrNoSuchIndex, tb.Def.Name, column)
	}
	rids, err := tree.Search(IndexKey(v))
	if err != nil {
		return nil, err
	}
	out := make([]ScanRow, 0, len(rids))
	for _, rid := range rids {
		row, err := d.Get(t, tb, rid)
		if err != nil {
			// The index can briefly outlive the row it points at if a
			// concurrent transaction deleted it; skip such entries.
			continue
		}
		out = append(out, ScanRow{RID: rid, Row: row})
	}
	return out, nil
}

// IndexRange scans the rows whose indexed column falls in [lo, hi]. Passing a
// null bound means "unbounded on that side".
func (d *DB) IndexRange(t *txn.Txn, tb *Table, column string, lo, hi Value, fn func(ScanRow) error) error {
	tree, _, ok := tb.IndexOn(column)
	if !ok {
		return fmt.Errorf("%w: no index on %s.%s", ErrNoSuchIndex, tb.Def.Name, column)
	}
	var start, end []byte
	if !lo.Null {
		start = IndexKey(lo)
	}
	if !hi.Null {
		// The tree's upper bound is exclusive; make it inclusive by appending
		// a byte that sorts after every key with this prefix.
		end = append(IndexKey(hi), 0xFF)
	}
	it := tree.Scan(start, end)
	for it.Next() {
		rid := it.RID()
		row, err := d.Get(t, tb, rid)
		if err != nil {
			continue
		}
		if err := fn(ScanRow{RID: rid, Row: row}); err != nil {
			if err == ErrStopScan {
				return nil
			}
			return err
		}
	}
	return it.Err()
}

// Count returns the number of rows in a table.
func (d *DB) Count(tb *Table) (int, error) { return tb.Heap.Count() }
