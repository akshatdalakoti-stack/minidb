package storage

// ---------------------------------------------------------------------------
// Phase 2: slotted pages.
//
// A slotted page decouples a record's identity (its slot number) from its
// location (a byte offset inside the page). The slot array grows upward from
// the header; record bytes are appended downward from the end of the page:
//
//	+---------------------------------------+ 0
//	| page header (32 bytes)                |
//	+---------------------------------------+ 32
//	| slot 0 | slot 1 | slot 2 | ...        |   4 bytes each: (offset, length)
//	+---------------------------------------+ <- header + numSlots*4
//	|                                       |
//	|            free space                 |
//	|                                       |
//	+---------------------------------------+ <- FreeEnd
//	| record 2                              |
//	| record 1                              |
//	| record 0                              |
//	+---------------------------------------+ PageSize
//
// Invariants:
//   - FreeEnd >= PageHeaderSize + NumSlots*SlotSize (the regions never overlap)
//   - a live slot's (offset, length) window lies entirely in [FreeEnd, PageSize)
//   - slot numbers are never reused for a different record while a RID that
//     points at them can still exist; deleting a record leaves a tombstone so
//     that the slot array (and therefore every other RID) stays stable.
//
// ---------------------------------------------------------------------------

// SlotSize is the width of one slot array entry: uint16 offset + uint16 length.
const SlotSize = 4

// MaxRecordSize is the largest record a page can hold (empty page, one slot).
const MaxRecordSize = PageSize - PageHeaderSize - SlotSize

// InitSlotted (re)initializes p as an empty slotted page of the given type.
func (p *Page) InitSlotted(t PageType) {
	p.Reset()
	p.SetType(t)
	p.setNumSlots(0)
	p.setFreeEnd(PageSize)
	p.SetNextPage(InvalidPageID)
	p.SetPrevPage(InvalidPageID)
	p.SetExtra(uint32(InvalidPageID))
}

func slotPos(slot uint16) int { return PageHeaderSize + int(slot)*SlotSize }

func (p *Page) slot(i uint16) (off, ln uint16) {
	b := p.Data[slotPos(i):]
	return uint16(b[0]) | uint16(b[1])<<8, uint16(b[2]) | uint16(b[3])<<8
}

func (p *Page) setSlot(i, off, ln uint16) {
	b := p.Data[slotPos(i):]
	b[0], b[1] = byte(off), byte(off>>8)
	b[2], b[3] = byte(ln), byte(ln>>8)
}

// slotArrayEnd is the first byte after the slot array.
func (p *Page) slotArrayEnd() int { return PageHeaderSize + int(p.NumSlots())*SlotSize }

// FreeSpace is the number of contiguous free bytes between the slot array and
// the record area. It does not include space wasted by deleted records; call
// Compact to reclaim that.
func (p *Page) FreeSpace() int {
	f := int(p.FreeEnd()) - p.slotArrayEnd()
	if f < 0 {
		return 0
	}
	return f
}

// UsedSpace is the number of bytes of live records plus their slots. B+tree
// split and merge decisions are made on this number rather than on a key
// count, because keys are variable length.
func (p *Page) UsedSpace() int {
	used := 0
	n := p.NumSlots()
	for i := uint16(0); i < n; i++ {
		off, ln := p.slot(i)
		if off == 0 {
			continue
		}
		used += int(ln) + SlotSize
	}
	return used
}
