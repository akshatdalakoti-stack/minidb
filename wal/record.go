// Package wal implements Phase 7: write-ahead logging.
//
// The contract the whole engine rests on is one sentence:
//
//	a page modification is described in the log, and that description is on
//	stable storage, BEFORE the modified page itself is allowed to reach disk.
//
// That single rule is what makes a crash survivable. The log is the truth; the
// data file is a cache of the log's effects. Two consequences follow:
//
//   - REDO is possible: any page change that never made it to the data file
//     can be replayed from the log after a restart.
//   - UNDO is possible: every record carries the information needed to reverse
//     it, so a transaction that never committed can be rolled back.
//
// The LSN of a record is its byte offset in the log file. "Read the record at
// LSN x" is therefore a seek, not a search, and comparing LSNs compares
// positions in time.
package wal

import (
	"minidb/storage"
)

// LSN is a log sequence number: the byte offset of a record in the log file.
type LSN = uint64

// InvalidLSN is the zero value; the log file starts with a header so no real
// record ever lives at offset 0.
const InvalidLSN LSN = 0

// Type identifies what a log record describes.
type Type uint8

const (
	TypeInvalid Type = iota

	// Transaction bookkeeping.
	TypeBegin
	TypeCommit
	TypeAbort
	TypeEnd // transaction fully rolled back / cleaned up

	// Physiological page operations: "apply this change to slot S of page P".
	// Redo re-applies the after image, undo re-applies the before image.
	TypeInsert
	TypeUpdate
	TypeDelete
	TypePageInit   // (re)initialize a page as a slotted page of a given type
	TypePageHeader // change one header field (next/prev/extra)
	TypeAllocPage  // a page was allocated for an object (redo-only)

	// Logical index operations. These exist so that a rollback can undo an
	// index change logically (call the tree's Delete) instead of physically:
	// by the time we roll back, a concurrent split may have moved the entry to
	// a different page, so the physical location in the log is stale.
	TypeIdxInsert
	TypeIdxDelete

	// Compensation log record: written while undoing, so that a crash during
	// rollback does not cause the undo to be repeated. Carries UndoNext, the
	// LSN the rollback should continue from.
	TypeCLR

	// Checkpoints.
	TypeCheckpointBegin
	TypeCheckpointEnd
)

func (t Type) String() string {
	switch t {
	case TypeBegin:
		return "BEGIN"
	case TypeCommit:
		return "COMMIT"
	case TypeAbort:
		return "ABORT"
	case TypeEnd:
		return "END"
	case TypeInsert:
		return "INSERT"
	case TypeUpdate:
		return "UPDATE"
	case TypeDelete:
		return "DELETE"
	case TypePageInit:
		return "PAGE_INIT"
	case TypePageHeader:
		return "PAGE_HDR"
	case TypeAllocPage:
		return "ALLOC_PAGE"
	case TypeIdxInsert:
		return "IDX_INSERT"
	case TypeIdxDelete:
		return "IDX_DELETE"
	case TypeCLR:
		return "CLR"
	case TypeCheckpointBegin:
		return "CKPT_BEGIN"
	case TypeCheckpointEnd:
		return "CKPT_END"
	}
	return "INVALID"
}

// Flags on a log record.
const (
	// FlagRedoOnly marks a record that must never be undone. Structural
	// changes to a B+tree (node splits, merges) are redo-only: a split is a
	// legal tree state no matter which transaction caused it, and other
	// transactions may already depend on it. This is the classic ARIES
	// "nested top action" treatment.
	FlagRedoOnly = 1 << 0

	// FlagShift marks an operation on an ordered page (a B+tree node), where
	// inserting or deleting shifts the slot array rather than leaving a
	// tombstone. Redo and undo have to reproduce that shifting exactly.
	FlagShift = 1 << 1
)

// Header field ids for TypePageHeader.
const (
	HdrNext = 1
	HdrPrev = 2
	HdrType = 3
	// HdrExtra is the page-type specific header word: the leftmost child of a
	// B+tree internal node, or the heap page's owner.
	HdrExtra = 4
)

// Record is one entry in the log.
type Record struct {
	LSN      LSN
	TxnID    uint64
	PrevLSN  LSN // previous record of the same transaction (the undo chain)
	UndoNext LSN // CLR only: where rollback continues
	Type     Type
	Op       uint8 // CLR: the operation to apply; PageInit: the page type; PageHeader: field id
	Op2      uint8 // CLR only: the Op of the record being compensated
	Flags    uint8
	PageID   storage.PageID
	Slot     uint16
	Before   []byte // undo image
	After    []byte // redo image
}
