package db

import (
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
