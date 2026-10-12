// Package index implements Phase 5: a disk-backed B+tree.
//
// Every node is a database page fetched through the buffer pool, so the tree
// works on data far larger than RAM - that is the entire point of a B+tree
// over a binary tree or a Go map. Its shape follows from the hardware: nodes
// are page sized because a page is the unit the disk hands us, and the fan-out
// (hundreds of keys per node) keeps the tree three or four levels deep even
// for millions of keys, so a lookup costs three or four page reads, most of
// which the buffer pool has cached.
//
//	            +-----------------+
//	            |  internal node  |          <- root, meta.Extra
//	            +--+-----+-----+--+
//	        ______/     |      \______
//	       /            |             \
//	+--------+     +--------+     +--------+
//	|  leaf  |<--->|  leaf  |<--->|  leaf  |  <- doubly linked
//	+--------+     +--------+     +--------+
//
// Only leaves hold data (key -> RID). Internal nodes hold separator keys and
// child pointers. Leaves are linked, so a range scan walks sideways instead of
// re-descending the tree for every key.
//
// Node layout. A node is an ordered slotted page: slot order is key order.
//
//	leaf slot i     = key bytes || RID (6 bytes)
//	internal slot i = key bytes || child page id (4 bytes)
//	internal Extra  = leftmost child (keys strictly less than slot 0's key)
//
// Keys are variable length byte strings compared with bytes.Compare, so the
// query layer can index integers (big-endian encoded) and strings with the
// same code. Split and merge decisions are therefore made on *byte
// occupancy*, not on a key count.
package index

import (
	"bytes"
	"errors"
	"sync"

	"minidb/buffer"
	"minidb/pageops"
	"minidb/storage"
)

// Errors returned by the index.
var (
	ErrKeyTooLarge = errors.New("index: key too large")
	ErrCorruptTree = errors.New("index: corrupt tree structure")
	ErrNotFound    = errors.New("index: key not found")
)

const (
	childSize = 4 // bytes of child page id in an internal entry

	// MaxKeySize keeps at least four entries per node, which is what makes
	// splitting always make progress.
	MaxKeySize = (storage.PageSize - storage.PageHeaderSize) / 4 / 2

	// nodeCapacity is the usable space in a node.
	nodeCapacity = storage.PageSize - storage.PageHeaderSize

	// mergeThreshold: a node holding less than this many bytes is a candidate
	// for merging with a sibling. A third of a page rather than the textbook
	// half, because merging is expensive and slightly under-full nodes are
	// harmless.
	mergeThreshold = nodeCapacity / 3
)

// Tree is a B+tree.
//
// Concurrency: a single tree-level RWMutex. Readers share it, writers take it
// exclusively. This is coarse - real engines use latch crabbing, taking and
// releasing per-node latches on the way down so that independent subtrees can
// be modified in parallel - but it is correct, and it composes safely with
// the buffer pool's per-frame latches because a reader and a writer can never
// be inside the tree at the same time. See DESIGN.md for the trade-off.
type Tree struct {
	mu   sync.RWMutex
	pool *buffer.Pool
	meta storage.PageID // page whose Extra field holds the current root
	name string
}

// Create builds a new, empty tree and returns it. The returned meta page id is
// what identifies the tree from then on (the catalog stores it).
func Create(ctx pageops.Ctx, name string) (*Tree, error) {
	metaH, err := ctx.NewPage(storage.PageTypeMeta)
	if err != nil {
		return nil, err
	}
	defer metaH.Release()

	rootH, err := ctx.NewPage(storage.PageTypeBTreeLeaf)
	if err != nil {
		return nil, err
	}
	root := rootH.ID()
	rootH.Release()

	if err := ctx.SetExtra(metaH, uint32(root)); err != nil {
		return nil, err
	}
	return &Tree{pool: ctx.Pool, meta: metaH.ID(), name: name}, nil
}

// Open attaches to an existing tree given its meta page.
func Open(pool *buffer.Pool, meta storage.PageID, name string) *Tree {
	return &Tree{pool: pool, meta: meta, name: name}
}

// MetaPage is the page id that identifies this tree.
func (t *Tree) MetaPage() storage.PageID { return t.meta }

// Name is the index name (for error messages and EXPLAIN-ish output).
func (t *Tree) Name() string { return t.name }

func (t *Tree) rootID() (storage.PageID, error) {
	h, err := t.pool.FetchRead(t.meta)
	if err != nil {
		return storage.InvalidPageID, err
	}
	defer h.Release()
	return storage.PageID(h.Page().Extra()), nil
}

// --- entry encoding --------------------------------------------------------

func leafEntry(key []byte, rid storage.RID) []byte {
	e := make([]byte, len(key)+storage.RIDSize)
	copy(e, key)
	storage.EncodeRID(e[len(key):], rid)
	return e
}

func leafKey(e []byte) []byte { return e[:len(e)-storage.RIDSize] }
func leafRID(e []byte) storage.RID {
	return storage.DecodeRID(e[len(e)-storage.RIDSize:])
}

func internalEntry(key []byte, child storage.PageID) []byte {
	e := make([]byte, len(key)+childSize)
	copy(e, key)
	e[len(key)+0] = byte(child)
	e[len(key)+1] = byte(child >> 8)
	e[len(key)+2] = byte(child >> 16)
	e[len(key)+3] = byte(child >> 24)
	return e
}

// internalKey is the separator stored in an internal entry. Separators are
// composite keys - the whole leaf entry, key||RID - not bare keys. With
// duplicate keys a bare separator is ambiguous: several leaves can hold the
// same key, and a descent comparing bare keys lands on an arbitrary one of
// them. Including the RID makes the ordering total, so "the leaf where this
// exact entry belongs" is always well defined.
func internalKey(e []byte) []byte { return e[:len(e)-childSize] }

// lowKey is the smallest composite key for a given user key: RID (page 0,
// slot 0) never identifies a real record, because page 0 is the meta page.
// Descending with it lands on the leftmost leaf that can hold the key.
func lowKey(key []byte) []byte { return leafEntry(key, storage.RID{}) }
func internalChild(e []byte) storage.PageID {
	b := e[len(e)-childSize:]
	return storage.PageID(uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24)
}

func isLeaf(p *storage.Page) bool { return p.Type() == storage.PageTypeBTreeLeaf }

// --- searching within a node ----------------------------------------------

// leafSearch finds where (key, rid) belongs in a leaf. Entries are ordered by
// key and then by RID, so duplicate keys are supported: an index on a
// non-unique column simply holds several entries with the same key.
func leafSearch(p *storage.Page, key []byte, rid storage.RID) (idx uint16, found bool, err error) {
	lo, hi := 0, int(p.NumSlots())
	for lo < hi {
		mid := (lo + hi) / 2
		e, err := p.Get(uint16(mid))
		if err != nil {
			return 0, false, err
		}
		c := bytes.Compare(leafKey(e), key)
		if c == 0 {
			c = leafRID(e).Compare(rid)
		}
		switch {
		case c < 0:
			lo = mid + 1
		case c > 0:
			hi = mid
		default:
			return uint16(mid), true, nil
		}
	}
	return uint16(lo), false, nil
}

// leafLowerBound finds the first entry with key >= the search key.
func leafLowerBound(p *storage.Page, key []byte) (uint16, error) {
	lo, hi := 0, int(p.NumSlots())
	for lo < hi {
		mid := (lo + hi) / 2
		e, err := p.Get(uint16(mid))
		if err != nil {
			return 0, err
		}
		if bytes.Compare(leafKey(e), key) < 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return uint16(lo), nil
}

// childFor returns the child of an internal node that a key belongs under, and
// the slot index of that child (-1 meaning the leftmost child in Extra).
func childFor(p *storage.Page, key []byte) (storage.PageID, int, error) {
	lo, hi := 0, int(p.NumSlots())
	for lo < hi {
		mid := (lo + hi) / 2
		e, err := p.Get(uint16(mid))
		if err != nil {
			return storage.InvalidPageID, 0, err
		}
		if bytes.Compare(internalKey(e), key) <= 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	idx := lo - 1
	if idx < 0 {
		return storage.PageID(p.Extra()), -1, nil
	}
	e, err := p.Get(uint16(idx))
	if err != nil {
		return storage.InvalidPageID, 0, err
	}
	return internalChild(e), idx, nil
}

// childAt returns the child pointer at a given index (-1 = Extra).
func childAt(p *storage.Page, idx int) (storage.PageID, error) {
	if idx < 0 {
		return storage.PageID(p.Extra()), nil
	}
	e, err := p.Get(uint16(idx))
	if err != nil {
		return storage.InvalidPageID, err
	}
	return internalChild(e), nil
}

// --- search ----------------------------------------------------------------

// Search returns every RID stored under key.
func (t *Tree) Search(key []byte) ([]storage.RID, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	leafID, err := t.findLeaf(lowKey(key))
	if err != nil {
		return nil, err
	}
	var out []storage.RID
	for leafID != storage.InvalidPageID {
		h, err := t.pool.FetchRead(leafID)
		if err != nil {
			return nil, err
		}
		p := h.Page()
		idx, err := leafLowerBound(p, key)
		if err != nil {
			h.Release()
			return nil, err
		}
		done := false
		for i := idx; i < p.NumSlots(); i++ {
			e, err := p.Get(i)
			if err != nil {
				h.Release()
				return nil, err
			}
			if !bytes.Equal(leafKey(e), key) {
				done = true
				break
			}
			out = append(out, leafRID(e))
		}
		next := p.NextPage()
		h.Release()
		if done {
			break
		}
		// The matching entries may continue into the next leaf.
		leafID = next
	}
	return out, nil
}

// findLeaf descends from the root to the leaf that would contain the given
// composite key (key||RID - see internalKey).
func (t *Tree) findLeaf(ckey []byte) (storage.PageID, error) {
	pid, err := t.rootID()
	if err != nil {
		return storage.InvalidPageID, err
	}
	for depth := 0; ; depth++ {
		if depth > 64 {
			return storage.InvalidPageID, ErrCorruptTree
		}
		h, err := t.pool.FetchRead(pid)
		if err != nil {
			return storage.InvalidPageID, err
		}
		p := h.Page()
		if isLeaf(p) {
			h.Release()
			return pid, nil
		}
		child, _, err := childFor(p, ckey)
		h.Release()
		if err != nil {
			return storage.InvalidPageID, err
		}
		if child == storage.InvalidPageID {
			return storage.InvalidPageID, ErrCorruptTree
		}
		pid = child
	}
}

// findLeftmostLeaf descends to the first leaf (used by full scans).
func (t *Tree) findLeftmostLeaf() (storage.PageID, error) {
	pid, err := t.rootID()
	if err != nil {
		return storage.InvalidPageID, err
	}
	for depth := 0; ; depth++ {
		if depth > 64 {
			return storage.InvalidPageID, ErrCorruptTree
		}
		h, err := t.pool.FetchRead(pid)
		if err != nil {
			return storage.InvalidPageID, err
		}
		p := h.Page()
		if isLeaf(p) {
			h.Release()
			return pid, nil
		}
		child, err := childAt(p, -1)
		h.Release()
		if err != nil {
			return storage.InvalidPageID, err
		}
		pid = child
	}
}

// --- insert ----------------------------------------------------------------
