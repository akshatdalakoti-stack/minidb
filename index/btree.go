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
	"fmt"
	"sync"

	"minidb/buffer"
	"minidb/pageops"
	"minidb/storage"
	"minidb/wal"
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

// Insert adds key -> rid to the tree.
//
// The logical operation is logged first (so a rollback can undo it by calling
// Delete), and the page changes underneath are logged as redo-only structural
// records.
func (t *Tree) Insert(ctx pageops.Ctx, key []byte, rid storage.RID) error {
	if len(key) > MaxKeySize {
		return fmt.Errorf("%w: %d bytes (max %d)", ErrKeyTooLarge, len(key), MaxKeySize)
	}
	t.mu.Lock()
	defer t.mu.Unlock()

	if ctx.Log != nil {
		if err := ctx.LogIndexOp(wal.TypeIdxInsert, t.meta, key, rid); err != nil {
			return err
		}
	}
	return t.insertUnlogged(ctx.Structural(), key, rid)
}

// insertUnlogged performs the physical insert. Rollback and recovery use it
// directly, because they supply their own compensation records.
func (t *Tree) insertUnlogged(ctx pageops.Ctx, key []byte, rid storage.RID) error {
	root, err := t.rootID()
	if err != nil {
		return err
	}
	sep, right, err := t.insertInto(ctx, root, key, rid)
	if err != nil || sep == nil {
		return err
	}
	// The root split: the tree grows by one level. This is the only way a
	// B+tree gets taller, which is why it stays perfectly balanced.
	newRootH, err := ctx.NewPage(storage.PageTypeBTreeInternal)
	if err != nil {
		return err
	}
	defer newRootH.Release()
	if err := ctx.SetExtra(newRootH, uint32(root)); err != nil {
		return err
	}
	if err := ctx.InsertShift(newRootH, 0, internalEntry(sep, right)); err != nil {
		return err
	}
	metaH, err := t.pool.FetchWrite(t.meta)
	if err != nil {
		return err
	}
	defer metaH.Release()
	return ctx.SetExtra(metaH, uint32(newRootH.ID()))
}

// insertInto inserts into the subtree rooted at pid. If the node had to split,
// it returns the separator key to install in the parent and the new right
// sibling.
func (t *Tree) insertInto(ctx pageops.Ctx, pid storage.PageID, key []byte, rid storage.RID) ([]byte, storage.PageID, error) {
	h, err := t.pool.FetchWrite(pid)
	if err != nil {
		return nil, storage.InvalidPageID, err
	}
	defer h.Release()
	p := h.Page()

	if isLeaf(p) {
		idx, found, err := leafSearch(p, key, rid)
		if err != nil {
			return nil, storage.InvalidPageID, err
		}
		if found {
			return nil, storage.InvalidPageID, nil // already present: idempotent
		}
		entry := leafEntry(key, rid)
		if fits(p, len(entry)) {
			return nil, storage.InvalidPageID, ctx.InsertShift(h, idx, entry)
		}
		return t.splitLeaf(ctx, h, idx, entry)
	}

	child, _, err := childFor(p, leafEntry(key, rid))
	if err != nil {
		return nil, storage.InvalidPageID, err
	}
	sep, right, err := t.insertInto(ctx, child, key, rid)
	if err != nil || sep == nil {
		return nil, storage.InvalidPageID, err
	}
	// The child split; install the separator here.
	idx, err := internalInsertPos(p, sep)
	if err != nil {
		return nil, storage.InvalidPageID, err
	}
	entry := internalEntry(sep, right)
	if fits(p, len(entry)) {
		return nil, storage.InvalidPageID, ctx.InsertShift(h, idx, entry)
	}
	return t.splitInternal(ctx, h, idx, entry)
}

// fits reports whether an entry of the given size can be added to a node.
func fits(p *storage.Page, entryLen int) bool {
	return p.FreeSpace()+p.DeadSpace() >= entryLen+storage.SlotSize
}

func internalInsertPos(p *storage.Page, key []byte) (uint16, error) {
	lo, hi := 0, int(p.NumSlots())
	for lo < hi {
		mid := (lo + hi) / 2
		e, err := p.Get(uint16(mid))
		if err != nil {
			return 0, err
		}
		if bytes.Compare(internalKey(e), key) < 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return uint16(lo), nil
}

// collectEntries copies every entry out of a node.
func collectEntries(p *storage.Page) ([][]byte, error) {
	n := p.NumSlots()
	out := make([][]byte, 0, n)
	for i := uint16(0); i < n; i++ {
		e, err := p.Get(i)
		if err != nil {
			return nil, err
		}
		out = append(out, append([]byte(nil), e...))
	}
	return out, nil
}

// splitPoint returns the index at which to cut a list of entries so that the
// two halves hold roughly the same number of bytes.
func splitPoint(entries [][]byte) int {
	total := 0
	for _, e := range entries {
		total += len(e) + storage.SlotSize
	}
	half, running := total/2, 0
	for i, e := range entries {
		running += len(e) + storage.SlotSize
		if running >= half {
			if i == 0 {
				return 1 // never produce an empty left node
			}
			if i == len(entries)-1 {
				return i // never produce an empty right node
			}
			return i + 1
		}
	}
	return len(entries) / 2
}

// keepCount converts a cut position in the augmented entry list into the
// number of original page entries the left node should keep: one fewer when
// the new entry itself landed on the left side.
func keepCount(cut int, idx uint16) int {
	if int(idx) < cut {
		return cut - 1
	}
	return cut
}

// trimLeft deletes every entry from position keep onwards, back to front.
func (t *Tree) trimLeft(ctx pageops.Ctx, h *buffer.Handle, keep int) error {
	for i := int(h.Page().NumSlots()) - 1; i >= keep; i-- {
		if err := ctx.DeleteShift(h, uint16(i)); err != nil {
			return err
		}
	}
	return nil
}

// replaceEntry overwrites an entry, falling back to delete+insert when the new
// entry is longer than the page can absorb in place. Separator keys can grow
// when a borrow moves a longer key up into the parent.
func replaceEntry(ctx pageops.Ctx, h *buffer.Handle, idx uint16, entry []byte) error {
	if h.Page().CanUpdate(idx, len(entry)) {
		return ctx.Update(h, idx, entry)
	}
	if err := ctx.DeleteShift(h, idx); err != nil {
		return err
	}
	return ctx.InsertShift(h, idx, entry)
}

// splitLeaf splits a full leaf, inserting `entry` at position idx in the
// process, and returns the separator to push into the parent.
func (t *Tree) splitLeaf(ctx pageops.Ctx, h *buffer.Handle, idx uint16, entry []byte) ([]byte, storage.PageID, error) {
	p := h.Page()
	entries, err := collectEntries(p)
	if err != nil {
		return nil, storage.InvalidPageID, err
	}
	entries = append(entries, nil)
	copy(entries[idx+1:], entries[idx:])
	entries[idx] = entry

	cut := splitPoint(entries)
	rightH, err := ctx.NewPage(storage.PageTypeBTreeLeaf)
	if err != nil {
		return nil, storage.InvalidPageID, err
	}
	defer rightH.Release()

	// Move the upper half into the new node.
	for i, e := range entries[cut:] {
		if err := ctx.InsertShift(rightH, uint16(i), e); err != nil {
			return nil, storage.InvalidPageID, err
		}
	}
	// Rewrite the left node. `entries` is the augmented list (it already
	// contains the new entry), so the number of original entries the left node
	// keeps is one fewer when the new entry landed on this side.
	if err := t.trimLeft(ctx, h, keepCount(cut, idx)); err != nil {
		return nil, storage.InvalidPageID, err
	}
	if int(idx) < cut {
		if err := ctx.InsertShift(h, idx, entry); err != nil {
			return nil, storage.InvalidPageID, err
		}
	}

	// Relink the leaf chain: left <-> right <-> oldNext.
	oldNext := p.NextPage()
	if err := ctx.SetNext(rightH, oldNext); err != nil {
		return nil, storage.InvalidPageID, err
	}
	if err := ctx.SetPrev(rightH, h.ID()); err != nil {
		return nil, storage.InvalidPageID, err
	}
	if err := ctx.SetNext(h, rightH.ID()); err != nil {
		return nil, storage.InvalidPageID, err
	}
	if oldNext != storage.InvalidPageID {
		nh, err := t.pool.FetchWrite(oldNext)
		if err != nil {
			return nil, storage.InvalidPageID, err
		}
		err = ctx.SetPrev(nh, rightH.ID())
		nh.Release()
		if err != nil {
			return nil, storage.InvalidPageID, err
		}
	}
	// For a leaf split the separator is a copy of the first entry on the
	// right (the entry itself stays in the leaf - all data lives in leaves).
	sep := append([]byte(nil), entries[cut]...)
	return sep, rightH.ID(), nil
}
