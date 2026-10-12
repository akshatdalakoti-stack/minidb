// Package pageops is the single choke point through which every logged page
// modification in the engine passes.
//
// Every mutation follows exactly the same three steps, in this order:
//
//  1. append a log record describing the change (with its before image)
//  2. apply the change to the page in the buffer pool
//  3. stamp the record's LSN onto the page header
//
// Step 3 is what makes recovery possible: the page LSN says "this page already
// reflects every change up to here", so redo can skip records it can see are
// already applied, and the buffer pool can ask the WAL "is the log durable up
// to this page's LSN?" before writing the page out.
//
// Having one implementation of that sequence - rather than one per subsystem -
// means the heap file and the B+tree cannot drift apart in how they log.
package pageops

import (
	"minidb/buffer"
	"minidb/storage"
	"minidb/wal"
)

// TxnState is the slice of a transaction that logging needs: who is writing,
// and where the head of its undo chain is. *txn.Txn implements it, and so does
// the lightweight stand-in that recovery uses when it rolls back the
// transactions it finds in the log of a crashed process.
type TxnState interface {
	ID() uint64
	LastLSN() wal.LSN
	SetLastLSN(wal.LSN)
}

// Ctx carries what a logged mutation needs: where the page lives, where the
// log is, and which transaction is responsible.
type Ctx struct {
	Pool *buffer.Pool
	Log  *wal.Manager
	Txn  TxnState // nil for system operations (bootstrap, structural work)

	// RedoOnly marks the records produced through this context as structural:
	// they are replayed by recovery but never undone. B+tree splits and merges
	// use this - a split is a legal state whichever transaction caused it, and
	// other transactions may already be relying on it.
	RedoOnly bool
}

// Structural returns a copy of the context whose records are redo-only.
func (c Ctx) Structural() Ctx { c.RedoOnly = true; return c }

// System returns a copy of the context with no owning transaction.
func (c Ctx) System() Ctx { c.Txn = nil; return c }

func (c Ctx) txnID() uint64 {
	if c.Txn == nil {
		return 0
	}
	return c.Txn.ID()
}

func (c Ctx) prevLSN() wal.LSN {
	if c.Txn == nil {
		return wal.InvalidLSN
	}
	return c.Txn.LastLSN()
}

func (c Ctx) flags() uint8 {
	if c.RedoOnly {
		return wal.FlagRedoOnly
	}
	return 0
}

// emit logs a record, applies it to the page and stamps the page LSN.
func (c Ctx) emit(h *buffer.Handle, r *wal.Record, apply func() error) error {
	r.TxnID = c.txnID()
	r.PrevLSN = c.prevLSN()
	r.Flags |= c.flags()
	r.PageID = h.ID()

	lsn, err := c.Log.Append(r) // 1. log first, always
	if err != nil {
		return err
	}
	h.MarkDirty()
	if err := apply(); err != nil { // 2. then change the page
		return err
	}
	h.Page().SetLSN(lsn) // 3. and record how far the page has come
	if c.Txn != nil {
		c.Txn.SetLastLSN(lsn)
	}
	return nil
}

// Insert adds a record to the page, choosing the slot.
func (c Ctx) Insert(h *buffer.Handle, rec storage.Record) (uint16, error) {
	// The slot has to be decided before the record is logged, because redo has
	// to reproduce the same RID. ReserveSlot answers "where would this go?"
	// without touching any slot, so the decision can be logged first.
	slot, err := h.Page().ReserveSlot(len(rec))
	if err != nil {
		return 0, err
	}
	r := &wal.Record{Type: wal.TypeInsert, Slot: slot, After: rec}
	if err := c.emit(h, r, func() error { return h.Page().InsertAt(slot, rec) }); err != nil {
		return 0, err
	}
	return slot, nil
}

// InsertAt adds a record at a specific slot.
func (c Ctx) InsertAt(h *buffer.Handle, slot uint16, rec storage.Record) error {
	r := &wal.Record{Type: wal.TypeInsert, Slot: slot, After: rec}
	return c.emit(h, r, func() error { return h.Page().InsertAt(slot, rec) })
}

// InsertShift inserts into an ordered page (a B+tree node), shifting the
// slot array right from slot onwards.
func (c Ctx) InsertShift(h *buffer.Handle, slot uint16, rec storage.Record) error {
	r := &wal.Record{Type: wal.TypeInsert, Slot: slot, After: rec, Flags: wal.FlagShift}
	return c.emit(h, r, func() error { return h.Page().InsertShift(slot, rec) })
}

// DeleteShift removes an entry from an ordered page, shifting the slot array
// left. The removed entry is logged so the delete can be reversed.
func (c Ctx) DeleteShift(h *buffer.Handle, slot uint16) error {
	old, err := h.Page().Get(slot)
	if err != nil {
		return err
	}
	before := append([]byte(nil), old...)
	r := &wal.Record{Type: wal.TypeDelete, Slot: slot, Before: before, Flags: wal.FlagShift}
	return c.emit(h, r, func() error { return h.Page().DeleteShift(slot) })
}

// Update replaces the record in a slot, logging the before image so the change
// can be rolled back.
func (c Ctx) Update(h *buffer.Handle, slot uint16, rec storage.Record) error {
	old, err := h.Page().Get(slot)
	if err != nil {
		return err
	}
	// Refuse before logging: an operation that is logged and then fails to
	// apply leaves a record that recovery could never replay.
	if !h.Page().CanUpdate(slot, len(rec)) {
		return storage.ErrPageFull
	}
	before := append([]byte(nil), old...)
	r := &wal.Record{Type: wal.TypeUpdate, Slot: slot, Before: before, After: rec}
	return c.emit(h, r, func() error { return h.Page().Update(slot, rec) })
}

// Delete removes the record in a slot, logging its contents so it can come back.
func (c Ctx) Delete(h *buffer.Handle, slot uint16) error {
	old, err := h.Page().Get(slot)
	if err != nil {
		return err
	}
	before := append([]byte(nil), old...)
	r := &wal.Record{Type: wal.TypeDelete, Slot: slot, Before: before}
	return c.emit(h, r, func() error { return h.Page().Delete(slot) })
}

// InitPage formats a page as an empty slotted page of the given type.
func (c Ctx) InitPage(h *buffer.Handle, t storage.PageType) error {
	r := &wal.Record{Type: wal.TypePageInit, Op: uint8(t), Flags: wal.FlagRedoOnly}
	return c.emit(h, r, func() error { h.Page().InitSlotted(t); return nil })
}

// SetNext, SetPrev and SetExtra change a single header field, logging the old
// value so the change is reversible. These are how page chains (heap file
// pages, B+tree leaf siblings) are maintained.
func (c Ctx) SetNext(h *buffer.Handle, id storage.PageID) error {
	return c.setHeader(h, wal.HdrNext, uint32(h.Page().NextPage()), uint32(id))
}

func (c Ctx) SetPrev(h *buffer.Handle, id storage.PageID) error {
	return c.setHeader(h, wal.HdrPrev, uint32(h.Page().PrevPage()), uint32(id))
}

func (c Ctx) SetExtra(h *buffer.Handle, v uint32) error {
	return c.setHeader(h, wal.HdrExtra, h.Page().Extra(), v)
}

func (c Ctx) setHeader(h *buffer.Handle, field uint8, old, new uint32) error {
	r := &wal.Record{
		Type:   wal.TypePageHeader,
		Op:     field,
		Before: wal.U32(old),
		After:  wal.U32(new),
	}
	return c.emit(h, r, func() error { return wal.Redo(h.Page(), r) })
}
