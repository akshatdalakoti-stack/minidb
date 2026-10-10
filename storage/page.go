// Package storage implements the lowest two layers of the engine: the fixed
// size page abstraction (Phase 1) and the slotted page layout that lets pages
// hold variable length records (Phase 2), plus the DiskManager that maps pages
// onto a single database file.
//
// Note: every page in the database - heap page, B+tree node, meta
// page - starts with the same 32 byte header. A uniform header means the WAL
// and the recovery manager can read/stamp a page LSN and verify a checksum on
// any page without knowing what kind of page it is.
package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
)

const (
	// PageSize is the fixed size of every page. 4 KiB matches the typical OS
	// page size and is a multiple of the 512B/4KiB sector size of real disks,
	// which is what makes a single page write mostly atomic in practice.
	PageSize = 4096
	// PageHeaderSize is the size of the common header every page carries.
	PageHeaderSize = 32
)

// Errors returned by the storage layer.
var (
	ErrCorruptPage    = errors.New("storage: page checksum mismatch (corrupt or torn page)")
	ErrPageFull       = errors.New("storage: not enough free space in page")
	ErrInvalidSlot    = errors.New("storage: invalid slot id")
	ErrRecordDeleted  = errors.New("storage: record has been deleted")
	ErrRecordTooLarge = errors.New("storage: record is larger than a page can ever hold")
	ErrInvalidPageID  = errors.New("storage: invalid page id")
)

// PageID identifies a page by its ordinal position in the database file.
// Page i lives at byte offset i*PageSize, so translating an ID to a file
// offset is a shift, not a lookup.
type PageID uint32

const (
	// InvalidPageID is the nil pointer of the storage layer.
	InvalidPageID PageID = 0xFFFFFFFF
	// MetaPageID is page 0, reserved for database metadata.
	MetaPageID PageID = 0
)

func (p PageID) String() string {
	if p == InvalidPageID {
		return "nil"
	}
	return fmt.Sprintf("p%d", uint32(p))
}

// PageType tags what a page is being used for. Stored in the header so that
// tools (and recovery) can walk a database file and make sense of it.
type PageType uint8

const (
	PageTypeInvalid PageType = iota
	PageTypeMeta
	PageTypeHeap
	PageTypeBTreeLeaf
	PageTypeBTreeInternal
	PageTypeFree
)

func (t PageType) String() string {
	switch t {
	case PageTypeMeta:
		return "meta"
	case PageTypeHeap:
		return "heap"
	case PageTypeBTreeLeaf:
		return "leaf"
	case PageTypeBTreeInternal:
		return "internal"
	case PageTypeFree:
		return "free"
	}
	return "invalid"
}

// Common header field offsets.
//
//	 0  LSN       uint64  log sequence number of the last update to this page
//	 8  Type      uint8
//	 9  Flags     uint8
//	10  NumSlots  uint16  number of slot entries (slotted pages)
//	12  FreeEnd   uint16  offset where the record area starts (grows downward)
//	14  --        uint16  reserved / padding
//	16  Next      uint32  next page (heap chain / leaf sibling / free list)
//	20  Prev      uint32  previous page
//	24  Extra     uint32  page-type specific (B+tree leftmost child, ...)
//	28  Checksum  uint32  CRC32C over the page with this field zeroed
const (
	offLSN      = 0
	offType     = 8
	offFlags    = 9
	offNumSlots = 10
	offFreeEnd  = 12
	offNext     = 16
	offPrev     = 20
	offExtra    = 24
	offChecksum = 28
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// Page is a raw, fixed size page buffer. It is deliberately a value type with
// an inline array: a page never grows, never reallocates, and a buffer pool
// frame can therefore own its memory for the lifetime of the pool.
type Page struct {
	Data [PageSize]byte
}

// Reset zeroes the page.
func (p *Page) Reset() { clear(p.Data[:]) }

// Bytes exposes the raw page buffer.
func (p *Page) Bytes() []byte { return p.Data[:] }

// LSN returns the log sequence number of the most recent log record that
// describes an update to this page. Recovery compares it against the LSN of a
// log record to decide whether the update is already reflected on the page.
func (p *Page) LSN() uint64        { return binary.LittleEndian.Uint64(p.Data[offLSN:]) }
func (p *Page) SetLSN(l uint64)    { binary.LittleEndian.PutUint64(p.Data[offLSN:], l) }
func (p *Page) Type() PageType     { return PageType(p.Data[offType]) }
func (p *Page) SetType(t PageType) { p.Data[offType] = byte(t) }
func (p *Page) Flags() uint8       { return p.Data[offFlags] }
func (p *Page) SetFlags(f uint8)   { p.Data[offFlags] = f }

func (p *Page) NumSlots() uint16     { return binary.LittleEndian.Uint16(p.Data[offNumSlots:]) }
func (p *Page) setNumSlots(n uint16) { binary.LittleEndian.PutUint16(p.Data[offNumSlots:], n) }
func (p *Page) FreeEnd() uint16      { return binary.LittleEndian.Uint16(p.Data[offFreeEnd:]) }
func (p *Page) setFreeEnd(n uint16)  { binary.LittleEndian.PutUint16(p.Data[offFreeEnd:], n) }

func (p *Page) NextPage() PageID      { return PageID(binary.LittleEndian.Uint32(p.Data[offNext:])) }
func (p *Page) SetNextPage(id PageID) { binary.LittleEndian.PutUint32(p.Data[offNext:], uint32(id)) }
func (p *Page) PrevPage() PageID      { return PageID(binary.LittleEndian.Uint32(p.Data[offPrev:])) }
func (p *Page) SetPrevPage(id PageID) { binary.LittleEndian.PutUint32(p.Data[offPrev:], uint32(id)) }
func (p *Page) Extra() uint32         { return binary.LittleEndian.Uint32(p.Data[offExtra:]) }
func (p *Page) SetExtra(v uint32)     { binary.LittleEndian.PutUint32(p.Data[offExtra:], v) }

// checksum is computed with the checksum field itself treated as zero, so the
// value can be stamped into the page it covers.
func (p *Page) computeChecksum() uint32 {
	h := crc32.New(crcTable)
	h.Write(p.Data[:offChecksum])
	var zero [4]byte
	h.Write(zero[:])
	h.Write(p.Data[offChecksum+4:])
	return h.Sum32()
}

// StampChecksum recomputes and writes the page checksum. The DiskManager calls
// this immediately before every write.
func (p *Page) StampChecksum() {
	binary.LittleEndian.PutUint32(p.Data[offChecksum:], p.computeChecksum())
}

// VerifyChecksum reports whether the page contents match the stored checksum.
// A mismatch means either bit rot or a torn write (the OS/disk wrote only part
// of the 4 KiB page before the machine died). Both are unrecoverable at this
// layer, so we surface them instead of handing out garbage.
func (p *Page) VerifyChecksum() error {
	stored := binary.LittleEndian.Uint32(p.Data[offChecksum:])
	if stored != p.computeChecksum() {
		return ErrCorruptPage
	}
	return nil
}
