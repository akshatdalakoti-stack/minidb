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
