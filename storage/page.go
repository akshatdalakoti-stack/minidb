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
	"errors"
	"fmt"
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
