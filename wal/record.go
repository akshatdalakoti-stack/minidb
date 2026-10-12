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
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"

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

func (r *Record) RedoOnly() bool { return r.Flags&FlagRedoOnly != 0 }

func (r *Record) String() string {
	return fmt.Sprintf("lsn=%d txn=%d prev=%d %s page=%v slot=%d before=%dB after=%dB",
		r.LSN, r.TxnID, r.PrevLSN, r.Type, r.PageID, r.Slot, len(r.Before), len(r.After))
}

// On-disk framing: uint32 payload length | payload | uint32 CRC32C of payload.
const (
	recHeaderSize = 48 // fixed part of the payload
	frameOverhead = 8  // length prefix + trailing checksum

	offRecLSN      = 0
	offRecTxn      = 8
	offRecPrev     = 16
	offRecUndoNext = 24
	offRecType     = 32
	offRecOp       = 33
	offRecFlags    = 34
	offRecOp2      = 35
	offRecPageID   = 36
	offRecSlot     = 40
	offRecBeforeLn = 44
	offRecAfterLn  = 46

	// MaxRecordSize caps a single log record; a page-sized before and after
	// image plus the header always fits comfortably.
	MaxRecordSize = 3 * storage.PageSize
)

var (
	// ErrTornRecord means the log ended in the middle of a record, or the
	// record's checksum does not match. Either way it is the tail of a log
	// that was being written when the machine died: recovery stops there.
	ErrTornRecord = errors.New("wal: torn or corrupt log record")
	ErrTooLarge   = errors.New("wal: log record too large")
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// EncodedSize is the number of bytes the record occupies in the log.
func (r *Record) EncodedSize() int {
	return frameOverhead + recHeaderSize + len(r.Before) + len(r.After)
}

// AppendTo serializes the record onto dst.
func (r *Record) AppendTo(dst []byte) ([]byte, error) {
	if len(r.Before) > 0xFFFF || len(r.After) > 0xFFFF {
		return nil, ErrTooLarge
	}
	payloadLen := recHeaderSize + len(r.Before) + len(r.After)
	if payloadLen+frameOverhead > MaxRecordSize {
		return nil, ErrTooLarge
	}
	var lenBuf [4]byte
	binary.LittleEndian.PutUint32(lenBuf[:], uint32(payloadLen))
	dst = append(dst, lenBuf[:]...)

	start := len(dst)
	dst = append(dst, make([]byte, recHeaderSize)...)
	h := dst[start:]
	binary.LittleEndian.PutUint64(h[offRecLSN:], r.LSN)
	binary.LittleEndian.PutUint64(h[offRecTxn:], r.TxnID)
	binary.LittleEndian.PutUint64(h[offRecPrev:], r.PrevLSN)
	binary.LittleEndian.PutUint64(h[offRecUndoNext:], r.UndoNext)
	h[offRecType] = byte(r.Type)
	h[offRecOp] = r.Op
	h[offRecFlags] = r.Flags
	h[offRecOp2] = r.Op2
	binary.LittleEndian.PutUint32(h[offRecPageID:], uint32(r.PageID))
	binary.LittleEndian.PutUint16(h[offRecSlot:], r.Slot)
	binary.LittleEndian.PutUint16(h[offRecBeforeLn:], uint16(len(r.Before)))
	binary.LittleEndian.PutUint16(h[offRecAfterLn:], uint16(len(r.After)))
	dst = append(dst, r.Before...)
	dst = append(dst, r.After...)

	crc := crc32.Checksum(dst[start:], crcTable)
	var crcBuf [4]byte
	binary.LittleEndian.PutUint32(crcBuf[:], crc)
	return append(dst, crcBuf[:]...), nil
}

// DecodeRecord parses one record from buf, which must start at a record
// boundary. It returns the record and the number of bytes consumed.
func DecodeRecord(buf []byte) (*Record, int, error) {
	if len(buf) < 4 {
		return nil, 0, ErrTornRecord
	}
	payloadLen := int(binary.LittleEndian.Uint32(buf))
	if payloadLen < recHeaderSize || payloadLen+frameOverhead > MaxRecordSize {
		return nil, 0, ErrTornRecord
	}
	total := payloadLen + frameOverhead
	if len(buf) < total {
		return nil, 0, ErrTornRecord
	}
	payload := buf[4 : 4+payloadLen]
	want := binary.LittleEndian.Uint32(buf[4+payloadLen:])
	if crc32.Checksum(payload, crcTable) != want {
		return nil, 0, ErrTornRecord
	}
	r := &Record{
		LSN:      binary.LittleEndian.Uint64(payload[offRecLSN:]),
		TxnID:    binary.LittleEndian.Uint64(payload[offRecTxn:]),
		PrevLSN:  binary.LittleEndian.Uint64(payload[offRecPrev:]),
		UndoNext: binary.LittleEndian.Uint64(payload[offRecUndoNext:]),
		Type:     Type(payload[offRecType]),
		Op:       payload[offRecOp],
		Op2:      payload[offRecOp2],
		Flags:    payload[offRecFlags],
		PageID:   storage.PageID(binary.LittleEndian.Uint32(payload[offRecPageID:])),
		Slot:     binary.LittleEndian.Uint16(payload[offRecSlot:]),
	}
	bl := int(binary.LittleEndian.Uint16(payload[offRecBeforeLn:]))
	al := int(binary.LittleEndian.Uint16(payload[offRecAfterLn:]))
	if recHeaderSize+bl+al != payloadLen {
		return nil, 0, ErrTornRecord
	}
	if bl > 0 {
		r.Before = append([]byte(nil), payload[recHeaderSize:recHeaderSize+bl]...)
	}
	if al > 0 {
		r.After = append([]byte(nil), payload[recHeaderSize+bl:recHeaderSize+bl+al]...)
	}
	return r, total, nil
}

// --- applying a record to a page ------------------------------------------
