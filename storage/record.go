package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// Record is an opaque, variable length byte string. The storage layer
// deliberately does not know what is inside a record: typing and schemas are
// the query layer's business. What storage guarantees is that whatever bytes
// you insert come back byte-for-byte under a stable RID.
type Record []byte

// RID ("record identifier") is the physical address of a record:
// the page it lives on plus the slot number inside that page. Indexes store
// RIDs, never offsets, because the slotted page layout is free to move a
// record's bytes around inside its page (compaction) without invalidating the
// RID.
type RID struct {
	PageID PageID
	Slot   uint16
}

// RIDSize is the encoded width of a RID: 4 byte page id + 2 byte slot.
const RIDSize = 6

// InvalidRID is the zero value used for "no record".
var InvalidRID = RID{PageID: InvalidPageID, Slot: 0xFFFF}

func (r RID) Valid() bool { return r.PageID != InvalidPageID && r.Slot != 0xFFFF }

func (r RID) String() string { return fmt.Sprintf("(%s,s%d)", r.PageID, r.Slot) }

// Compare orders RIDs by (page, slot); used to make scans deterministic.
func (r RID) Compare(o RID) int {
	switch {
	case r.PageID < o.PageID:
		return -1
	case r.PageID > o.PageID:
		return 1
	case r.Slot < o.Slot:
		return -1
	case r.Slot > o.Slot:
		return 1
	}
	return 0
}

// EncodeRID writes r into dst (which must be at least RIDSize bytes).
func EncodeRID(dst []byte, r RID) {
	binary.LittleEndian.PutUint32(dst, uint32(r.PageID))
	binary.LittleEndian.PutUint16(dst[4:], r.Slot)
}

// DecodeRID reads a RID from the first RIDSize bytes of b.
func DecodeRID(b []byte) RID {
	return RID{
		PageID: PageID(binary.LittleEndian.Uint32(b)),
		Slot:   binary.LittleEndian.Uint16(b[4:]),
	}
}

// ErrBadRecord is returned when a record cannot be decoded.
var ErrBadRecord = errors.New("storage: malformed record")

// EncodeFields serializes a list of variable length fields into a single
// self-describing record:
//
//	uint16 nfields | uint16 len[0] .. len[n-1] | field bytes concatenated
//
// This is the generic "tuple" framing used by the heap table and the query
// layer. Lengths are 16 bit because a record can never exceed a page anyway.
func EncodeFields(fields [][]byte) (Record, error) {
	if len(fields) > 0xFFFF {
		return nil, ErrBadRecord
	}
	total := 2 + 2*len(fields)
	for _, f := range fields {
		if len(f) > 0xFFFF {
			return nil, ErrBadRecord
		}
		total += len(f)
	}
	buf := make([]byte, total)
	binary.LittleEndian.PutUint16(buf, uint16(len(fields)))
	off := 2
	for _, f := range fields {
		binary.LittleEndian.PutUint16(buf[off:], uint16(len(f)))
		off += 2
	}
	for _, f := range fields {
		copy(buf[off:], f)
		off += len(f)
	}
	return buf, nil
}
