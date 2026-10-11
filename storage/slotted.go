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

// DeadSpace is the number of bytes that Compact would reclaim.
func (p *Page) DeadSpace() int {
	live := 0
	n := p.NumSlots()
	for i := uint16(0); i < n; i++ {
		off, ln := p.slot(i)
		if off == 0 {
			continue
		}
		live += int(ln)
	}
	return (PageSize - int(p.FreeEnd())) - live
}

// SlotOccupied reports whether slot i currently holds a live record.
func (p *Page) SlotOccupied(i uint16) bool {
	if i >= p.NumSlots() {
		return false
	}
	off, _ := p.slot(i)
	return off != 0
}

// Get returns the record stored in slot i. The returned slice aliases the page
// buffer: it is valid only while the caller holds the page pinned, and callers
// that hand the bytes to someone else must copy first.
func (p *Page) Get(i uint16) (Record, error) {
	if i >= p.NumSlots() {
		return nil, ErrInvalidSlot
	}
	off, ln := p.slot(i)
	if off == 0 {
		return nil, ErrRecordDeleted
	}
	if int(off)+int(ln) > PageSize || int(off) < p.slotArrayEnd() {
		return nil, ErrCorruptPage
	}
	return Record(p.Data[off : int(off)+int(ln)]), nil
}

// Insert stores rec in the first free slot (reusing a tombstone if one exists)
// and returns that slot number. It returns ErrPageFull if the record does not
// fit even after compaction.
func (p *Page) Insert(rec Record) (uint16, error) {
	if len(rec) > MaxRecordSize {
		return 0, ErrRecordTooLarge
	}
	slot, found := p.freeSlot()
	need := len(rec)
	if !found {
		need += SlotSize
	}
	if p.FreeSpace() < need {
		p.Compact()
		if p.FreeSpace() < need {
			return 0, ErrPageFull
		}
	}
	if !found {
		slot = p.NumSlots()
		p.setNumSlots(slot + 1)
	}
	p.place(slot, rec)
	return slot, nil
}

// ReserveSlot reports which slot an Insert of a record of the given length
// would use, compacting the page if that is what it takes to make room. It
// changes no slot contents, so the caller can log the decision before
// performing it - which is exactly what the write-ahead log needs, since redo
// must reproduce the same record id.
func (p *Page) ReserveSlot(recLen int) (uint16, error) {
	if recLen > MaxRecordSize {
		return 0, ErrRecordTooLarge
	}
	slot, found := p.freeSlot()
	need := recLen
	if !found {
		need += SlotSize
		slot = p.NumSlots()
	}
	if p.FreeSpace() < need {
		p.Compact() // pure physical rearrangement: slot numbers are preserved
		if p.FreeSpace() < need {
			return 0, ErrPageFull
		}
	}
	return slot, nil
}

// InsertAt stores rec in a specific slot, growing the slot array if needed.
// Recovery uses this: a redo must reproduce the exact RID the original
// operation produced, so the slot number comes from the log record rather than
// being re-derived.
func (p *Page) InsertAt(slot uint16, rec Record) error {
	if len(rec) > MaxRecordSize {
		return ErrRecordTooLarge
	}
	n := p.NumSlots()
	if slot < n {
		if off, _ := p.slot(slot); off != 0 {
			// Slot already live: treat as an overwrite.
			return p.Update(slot, rec)
		}
	}
	grow := 0
	if slot >= n {
		grow = (int(slot) - int(n) + 1) * SlotSize
	}
	if p.FreeSpace() < len(rec)+grow {
		p.Compact()
		if p.FreeSpace() < len(rec)+grow {
			return ErrPageFull
		}
	}
	if slot >= n {
		for i := int(n); i <= int(slot); i++ {
			p.setSlot(uint16(i), 0, 0) // new slots start life as tombstones
		}
		p.setNumSlots(slot + 1)
	}
	p.place(slot, rec)
	return nil
}

// place copies rec into the record area and points slot at it. The caller must
// have verified there is room.
func (p *Page) place(slot uint16, rec Record) {
	end := p.FreeEnd() - uint16(len(rec))
	copy(p.Data[end:], rec)
	p.setFreeEnd(end)
	p.setSlot(slot, end, uint16(len(rec)))
}

func (p *Page) freeSlot() (uint16, bool) {
	n := p.NumSlots()
	for i := uint16(0); i < n; i++ {
		if off, ln := p.slot(i); off == 0 && ln == 0 {
			return i, true
		}
	}
	return 0, false
}

// --- ordered pages ---------------------------------------------------------
//
// A heap page treats slot numbers as record identity, so a delete leaves a
// tombstone. A B+tree node needs the opposite: slot order is key order, and
// nothing outside the node points at a slot number. InsertShift/DeleteShift
// give those pages an ordered slot array, where inserting in the middle shifts
// the 4 byte slot entries (not the records) to make room.

// InsertShift inserts rec at slot i, shifting slots i.. one position right.
func (p *Page) InsertShift(i uint16, rec Record) error {
	n := p.NumSlots()
	if i > n {
		return ErrInvalidSlot
	}
	if len(rec) > MaxRecordSize {
		return ErrRecordTooLarge
	}
	if p.FreeSpace() < len(rec)+SlotSize {
		p.Compact()
		if p.FreeSpace() < len(rec)+SlotSize {
			return ErrPageFull
		}
	}
	// Shift the slot array right by one entry, back to front.
	copy(p.Data[slotPos(i+1):slotPos(n+1)], p.Data[slotPos(i):slotPos(n)])
	p.setNumSlots(n + 1)
	p.place(i, rec)
	return nil
}

// DeleteShift removes slot i entirely, shifting slots i+1.. one position left.
func (p *Page) DeleteShift(i uint16) error {
	n := p.NumSlots()
	if i >= n {
		return ErrInvalidSlot
	}
	copy(p.Data[slotPos(i):slotPos(n-1)], p.Data[slotPos(i+1):slotPos(n)])
	p.setSlot(n-1, 0, 0)
	p.setNumSlots(n - 1)
	return nil
}

// Delete tombstones slot i. The slot itself is kept so record ids stay stable;
// the bytes are reclaimed on the next Compact.
func (p *Page) Delete(i uint16) error {
	if i >= p.NumSlots() {
		return ErrInvalidSlot
	}
	off, _ := p.slot(i)
	if off == 0 {
		return ErrRecordDeleted
	}
	p.setSlot(i, 0, 0)
	return nil
}

// Update replaces the record in slot i.
//
//   - shrinking or same size: written in place, the tail becomes dead space
//   - growing: needs free space for a fresh copy (compacting first if needed);
//     if the page cannot hold the bigger record, ErrPageFull is returned with
//     the old value untouched so the caller can relocate the record.
func (p *Page) Update(i uint16, rec Record) error {
	if i >= p.NumSlots() {
		return ErrInvalidSlot
	}
	if len(rec) > MaxRecordSize {
		return ErrRecordTooLarge
	}
	off, ln := p.slot(i)
	if off == 0 {
		return ErrRecordDeleted
	}
	if len(rec) <= int(ln) {
		copy(p.Data[off:], rec)
		p.setSlot(i, off, uint16(len(rec)))
		return nil
	}
	if p.FreeSpace() < len(rec) {
		p.Compact()
		if p.FreeSpace() < len(rec) {
			return ErrPageFull
		}
	}
	p.place(i, rec)
	return nil
}

// CanUpdate reports whether Update(i, recLen bytes) would succeed.
//
// The write-ahead log needs this: a record is written to the log before it
// is applied, so an operation that might fail on application must be refused
// before anything is logged. Otherwise the log would contain a change that
// recovery cannot replay.
//
// The answer depends only on the page's logical contents (which records are
// live and how big they are), never on their physical arrangement, so it gives
// the same answer at redo time as it did originally.
func (p *Page) CanUpdate(i uint16, recLen int) bool {
	if i >= p.NumSlots() || recLen > MaxRecordSize {
		return false
	}
	off, ln := p.slot(i)
	if off == 0 {
		return false
	}
	if recLen <= int(ln) {
		return true
	}
	return p.FreeSpace()+p.DeadSpace() >= recLen
}

// Compact slides every live record to the end of the page, eliminating the
// holes left by deletes and in-place shrinks. Slot numbers are preserved, so
// no RID is invalidated - that is the whole point of the indirection.
func (p *Page) Compact() {
	var tmp [PageSize]byte
	end := PageSize
	n := p.NumSlots()
	for i := uint16(0); i < n; i++ {
		off, ln := p.slot(i)
		if off == 0 {
			continue
		}
		end -= int(ln)
		copy(tmp[end:], p.Data[off:int(off)+int(ln)])
		p.setSlot(i, uint16(end), ln)
	}
	copy(p.Data[end:], tmp[end:])
	clear(p.Data[p.slotArrayEnd():end]) // do not leak deleted bytes
	p.setFreeEnd(uint16(end))
}

// Validate performs a structural self-check of the slotted layout. Tests use
// it, and so does the recovery path when a page is read back from disk.
func (p *Page) Validate() error {
	n := p.NumSlots()
	if slotPos(n) > PageSize {
		return ErrCorruptPage
	}
	if int(p.FreeEnd()) > PageSize || int(p.FreeEnd()) < slotPos(n) {
		return ErrCorruptPage
	}
	for i := uint16(0); i < n; i++ {
		off, ln := p.slot(i)
		if off == 0 {
			continue
		}
		if int(off) < int(p.FreeEnd()) || int(off)+int(ln) > PageSize {
			return ErrCorruptPage
		}
	}
	return nil
}
