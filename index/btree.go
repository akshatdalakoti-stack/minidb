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

// splitInternal splits a full internal node. Unlike a leaf split, the middle
// key moves up rather than being copied: it is a separator, not data.
func (t *Tree) splitInternal(ctx pageops.Ctx, h *buffer.Handle, idx uint16, entry []byte) ([]byte, storage.PageID, error) {
	p := h.Page()
	entries, err := collectEntries(p)
	if err != nil {
		return nil, storage.InvalidPageID, err
	}
	entries = append(entries, nil)
	copy(entries[idx+1:], entries[idx:])
	entries[idx] = entry

	cut := splitPoint(entries)
	if cut >= len(entries) {
		cut = len(entries) - 1
	}
	promoted := entries[cut] // moves up
	rightH, err := ctx.NewPage(storage.PageTypeBTreeInternal)
	if err != nil {
		return nil, storage.InvalidPageID, err
	}
	defer rightH.Release()

	// The promoted entry's child becomes the right node's leftmost child.
	if err := ctx.SetExtra(rightH, uint32(internalChild(promoted))); err != nil {
		return nil, storage.InvalidPageID, err
	}
	for i, e := range entries[cut+1:] {
		if err := ctx.InsertShift(rightH, uint16(i), e); err != nil {
			return nil, storage.InvalidPageID, err
		}
	}
	if err := t.trimLeft(ctx, h, keepCount(cut, idx)); err != nil {
		return nil, storage.InvalidPageID, err
	}
	if int(idx) < cut {
		if err := ctx.InsertShift(h, idx, entry); err != nil {
			return nil, storage.InvalidPageID, err
		}
	}
	sep := append([]byte(nil), internalKey(promoted)...)
	return sep, rightH.ID(), nil
}

// --- delete ----------------------------------------------------------------

// Delete removes key -> rid. It reports whether the entry was there.
func (t *Tree) Delete(ctx pageops.Ctx, key []byte, rid storage.RID) (bool, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	root, err := t.rootID()
	if err != nil {
		return false, err
	}
	// Log the logical operation before touching anything, so an abort can put
	// the entry back.
	if ctx.Log != nil {
		if err := ctx.LogIndexOp(wal.TypeIdxDelete, t.meta, key, rid); err != nil {
			return false, err
		}
	}
	sctx := ctx.Structural()
	found, err := t.deleteFrom(sctx, root, key, rid)
	if err != nil || !found {
		return found, err
	}
	return true, t.shrinkRoot(sctx)
}

// deleteUnlogged is the physical delete used by rollback and recovery.
func (t *Tree) deleteUnlogged(ctx pageops.Ctx, key []byte, rid storage.RID) (bool, error) {
	root, err := t.rootID()
	if err != nil {
		return false, err
	}
	found, err := t.deleteFrom(ctx, root, key, rid)
	if err != nil || !found {
		return found, err
	}
	return true, t.shrinkRoot(ctx)
}

// UndoInsert and UndoDelete are the logical inverses used when a transaction
// rolls back or when recovery undoes a loser. They do the tree work but log
// only redo-only structural records: the caller writes the compensation record
// that moves the transaction's undo cursor past the original operation.
func (t *Tree) UndoInsert(ctx pageops.Ctx, key []byte, rid storage.RID) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	_, err := t.deleteUnlogged(ctx.Structural(), key, rid)
	return err
}

func (t *Tree) UndoDelete(ctx pageops.Ctx, key []byte, rid storage.RID) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.insertUnlogged(ctx.Structural(), key, rid)
}

// shrinkRoot collapses a root that has lost all its separators: its only
// remaining child becomes the new root and the tree loses a level.
func (t *Tree) shrinkRoot(ctx pageops.Ctx) error {
	root, err := t.rootID()
	if err != nil {
		return err
	}
	h, err := t.pool.FetchRead(root)
	if err != nil {
		return err
	}
	p := h.Page()
	if isLeaf(p) || p.NumSlots() > 0 {
		h.Release()
		return nil
	}
	only := storage.PageID(p.Extra())
	h.Release()
	if only == storage.InvalidPageID {
		return ErrCorruptTree
	}
	metaH, err := t.pool.FetchWrite(t.meta)
	if err != nil {
		return err
	}
	defer metaH.Release()
	return ctx.SetExtra(metaH, uint32(only))
}

func (t *Tree) deleteFrom(ctx pageops.Ctx, pid storage.PageID, key []byte, rid storage.RID) (bool, error) {
	h, err := t.pool.FetchWrite(pid)
	if err != nil {
		return false, err
	}
	p := h.Page()

	if isLeaf(p) {
		idx, found, err := leafSearch(p, key, rid)
		if err != nil || !found {
			h.Release()
			return false, err
		}
		err = ctx.DeleteShift(h, idx)
		h.Release()
		return err == nil, err
	}

	child, childIdx, err := childFor(p, leafEntry(key, rid))
	if err != nil {
		h.Release()
		return false, err
	}
	found, err := t.deleteFrom(ctx, child, key, rid)
	if err != nil || !found {
		h.Release()
		return found, err
	}
	err = t.rebalance(ctx, h, childIdx, child)
	h.Release()
	return true, err
}

// rebalance restores the occupancy invariant for a child that may have become
// too empty, by borrowing an entry from a sibling or merging with one.
func (t *Tree) rebalance(ctx pageops.Ctx, parent *buffer.Handle, childIdx int, childID storage.PageID) error {
	ch, err := t.pool.FetchWrite(childID)
	if err != nil {
		return err
	}
	if ch.Page().UsedSpace() >= mergeThreshold {
		ch.Release()
		return nil
	}
	pp := parent.Page()
	nSlots := int(pp.NumSlots())

	// Prefer the right sibling; it makes the index bookkeeping simpler.
	if childIdx+1 < nSlots {
		sibID, err := childAt(pp, childIdx+1)
		if err != nil {
			ch.Release()
			return err
		}
		sh, err := t.pool.FetchWrite(sibID)
		if err != nil {
			ch.Release()
			return err
		}
		err = t.mergeOrBorrowRight(ctx, parent, childIdx+1, ch, sh)
		sh.Release()
		ch.Release()
		return err
	}
	if childIdx >= 0 {
		sibID, err := childAt(pp, childIdx-1)
		if err != nil {
			ch.Release()
			return err
		}
		sh, err := t.pool.FetchWrite(sibID)
		if err != nil {
			ch.Release()
			return err
		}
		// Left sibling: the roles swap - the sibling is the left node and the
		// child is the right one, separated by the entry at childIdx.
		err = t.mergeOrBorrowRight(ctx, parent, childIdx, sh, ch)
		sh.Release()
		ch.Release()
		return err
	}
	return nil // only child of the root: nothing to balance against
}

// mergeOrBorrowRight handles a (left, right) sibling pair whose separator is
// the parent entry at sepIdx.
func (t *Tree) mergeOrBorrowRight(ctx pageops.Ctx, parent *buffer.Handle, sepIdx int, left, right *buffer.Handle) error {
	lp, rp := left.Page(), right.Page()
	if lp.UsedSpace()+rp.UsedSpace() <= nodeCapacity-storage.PageHeaderSize {
		return t.merge(ctx, parent, sepIdx, left, right)
	}
	return t.borrow(ctx, parent, sepIdx, left, right)
}

// merge folds the right node into the left one and removes the separator from
// the parent.
//
// The emptied page is deliberately not returned to the disk free list: log
// records that mention it may still be replayed by recovery, and handing the
// page straight back to the allocator would let those replays land on whatever
// object gets the page next. Leaving it allocated but unreachable is safe and
// costs only space. DESIGN.md records this as a known limitation.
func (t *Tree) merge(ctx pageops.Ctx, parent *buffer.Handle, sepIdx int, left, right *buffer.Handle) error {
	lp, rp := left.Page(), right.Page()
	sepEntry, err := parent.Page().Get(uint16(sepIdx))
	if err != nil {
		return err
	}
	sepKey := append([]byte(nil), internalKey(sepEntry)...)

	if !isLeaf(lp) {
		// For internal nodes the separator comes back down and pairs up with
		// the right node's leftmost child.
		if err := ctx.InsertShift(left, lp.NumSlots(), internalEntry(sepKey, storage.PageID(rp.Extra()))); err != nil {
			return err
		}
	}
	entries, err := collectEntries(rp)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if err := ctx.InsertShift(left, lp.NumSlots(), e); err != nil {
			return err
		}
	}
	for i := int(rp.NumSlots()) - 1; i >= 0; i-- {
		if err := ctx.DeleteShift(right, uint16(i)); err != nil {
			return err
		}
	}
	if isLeaf(lp) {
		// Unlink the emptied leaf from the sibling chain.
		next := rp.NextPage()
		if err := ctx.SetNext(left, next); err != nil {
			return err
		}
		if next != storage.InvalidPageID {
			nh, err := t.pool.FetchWrite(next)
			if err != nil {
				return err
			}
			err = ctx.SetPrev(nh, left.ID())
			nh.Release()
			if err != nil {
				return err
			}
		}
	}
	return ctx.DeleteShift(parent, uint16(sepIdx))
}

// borrow moves one entry across the sibling boundary and fixes the separator.
func (t *Tree) borrow(ctx pageops.Ctx, parent *buffer.Handle, sepIdx int, left, right *buffer.Handle) error {
	lp, rp := left.Page(), right.Page()
	sepEntry, err := parent.Page().Get(uint16(sepIdx))
	if err != nil {
		return err
	}
	sepKey := append([]byte(nil), internalKey(sepEntry)...)

	if lp.UsedSpace() < rp.UsedSpace() {
		if rp.NumSlots() < 2 {
			return nil // donor cannot spare an entry; leave the node under-full
		}
		// Move the right node's first entry to the end of the left node.
		first, err := rp.Get(0)
		if err != nil {
			return err
		}
		e := append([]byte(nil), first...)
		var newSep []byte
		if isLeaf(lp) {
			if err := ctx.InsertShift(left, lp.NumSlots(), e); err != nil {
				return err
			}
			next, err := rp.Get(1)
			if err != nil {
				return err
			}
			newSep = append([]byte(nil), next...)
		} else {
			// Internal: the separator descends into the left node paired with
			// the right node's leftmost child, and the moved key becomes the
			// new separator.
			if err := ctx.InsertShift(left, lp.NumSlots(), internalEntry(sepKey, storage.PageID(rp.Extra()))); err != nil {
				return err
			}
			if err := ctx.SetExtra(right, uint32(internalChild(e))); err != nil {
				return err
			}
			newSep = append([]byte(nil), internalKey(e)...)
		}
		if err := ctx.DeleteShift(right, 0); err != nil {
			return err
		}
		return replaceEntry(ctx, parent, uint16(sepIdx), internalEntry(newSep, right.ID()))
	}

	// Move the left node's last entry to the front of the right node.
	if lp.NumSlots() < 2 {
		return nil
	}
	lastIdx := lp.NumSlots() - 1
	last, err := lp.Get(lastIdx)
	if err != nil {
		return err
	}
	e := append([]byte(nil), last...)
	var newSep []byte
	if isLeaf(lp) {
		if err := ctx.InsertShift(right, 0, e); err != nil {
			return err
		}
		newSep = append([]byte(nil), e...)
	} else {
		if err := ctx.InsertShift(right, 0, internalEntry(sepKey, storage.PageID(rp.Extra()))); err != nil {
			return err
		}
		if err := ctx.SetExtra(right, uint32(internalChild(e))); err != nil {
			return err
		}
		newSep = append([]byte(nil), internalKey(e)...)
	}
	if err := ctx.DeleteShift(left, lastIdx); err != nil {
		return err
	}
	return replaceEntry(ctx, parent, uint16(sepIdx), internalEntry(newSep, right.ID()))
}

// --- range scan ------------------------------------------------------------

// Iterator walks the leaf chain in key order.
//
// It deliberately does not hold the tree lock between calls to Next: a scan can
// last for a long time and blocking every writer for its duration would be a
// terrible trade. Instead it copies a batch of entries out of the current leaf
// under a read lock, serves Next from that buffer, and re-positions itself by
// key when the buffer runs dry. A concurrent split or merge therefore cannot
// corrupt a scan; at worst the scan sees a value that was inserted after it
// started.
type Iterator struct {
	tree    *Tree
	end     []byte // exclusive upper bound; nil means "to the end"
	batch   []kv
	pos     int
	lastKey []byte
	lastRID storage.RID
	started bool
	done    bool
	err     error
}

type kv struct {
	key []byte
	rid storage.RID
}

const scanBatch = 256

// Scan returns an iterator over [start, end). A nil start means "from the
// beginning"; a nil end means "to the end".
func (t *Tree) Scan(start, end []byte) *Iterator {
	it := &Iterator{tree: t, end: end}
	if start != nil {
		it.lastKey = append([]byte(nil), start...)
	}
	return it
}

// Next advances the iterator. It returns false at the end of the range or on
// error (check Err).
func (it *Iterator) Next() bool {
	if it.done {
		return false
	}
	if it.pos < len(it.batch) {
		e := it.batch[it.pos]
		it.pos++
		it.lastKey, it.lastRID, it.started = e.key, e.rid, true
		return true
	}
	if err := it.fill(); err != nil {
		it.err = err
		it.done = true
		return false
	}
	if len(it.batch) == 0 {
		it.done = true
		return false
	}
	return it.Next()
}

// fill loads the next batch of entries, starting just after the last one
// returned.
func (it *Iterator) fill() error {
	t := it.tree
	t.mu.RLock()
	defer t.mu.RUnlock()

	it.batch = it.batch[:0]
	it.pos = 0

	var leafID storage.PageID
	var err error
	if it.lastKey == nil {
		leafID, err = t.findLeftmostLeaf()
	} else {
		leafID, err = t.findLeaf(lowKey(it.lastKey))
	}
	if err != nil {
		return err
	}

	for leafID != storage.InvalidPageID && len(it.batch) < scanBatch {
		h, err := t.pool.FetchRead(leafID)
		if err != nil {
			return err
		}
		p := h.Page()
		start := uint16(0)
		if it.lastKey != nil {
			start, err = leafLowerBound(p, it.lastKey)
			if err != nil {
				h.Release()
				return err
			}
		}
		for i := start; i < p.NumSlots() && len(it.batch) < scanBatch; i++ {
			e, err := p.Get(i)
			if err != nil {
				h.Release()
				return err
			}
			key, rid := leafKey(e), leafRID(e)
			// Skip everything up to and including the last entry we returned.
			if it.started {
				if c := bytes.Compare(key, it.lastKey); c < 0 {
					continue
				} else if c == 0 && rid.Compare(it.lastRID) <= 0 {
					continue
				}
			}
			if it.end != nil && bytes.Compare(key, it.end) >= 0 {
				h.Release()
				return nil // past the end of the range
			}
			it.batch = append(it.batch, kv{append([]byte(nil), key...), rid})
		}
		next := p.NextPage()
		h.Release()
		if len(it.batch) >= scanBatch {
			return nil
		}
		leafID = next
		if leafID != storage.InvalidPageID && len(it.batch) > 0 {
			// Continue into the next leaf from its very beginning.
			last := it.batch[len(it.batch)-1]
			it.lastKey, it.lastRID, it.started = last.key, last.rid, true
		}
	}
	return nil
}

// Key is the current key. Valid after a Next that returned true.
func (it *Iterator) Key() []byte { return it.lastKey }

// RID is the current record id.
func (it *Iterator) RID() storage.RID { return it.lastRID }

// Err returns the error that stopped the iteration, if any.
func (it *Iterator) Err() error { return it.err }

// --- maintenance / introspection -------------------------------------------

// Height returns the number of levels in the tree (1 = a single leaf).
func (t *Tree) Height() (int, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	pid, err := t.rootID()
	if err != nil {
		return 0, err
	}
	h := 0
	for {
		ph, err := t.pool.FetchRead(pid)
		if err != nil {
			return 0, err
		}
		h++
		p := ph.Page()
		if isLeaf(p) {
			ph.Release()
			return h, nil
		}
		child, err := childAt(p, -1)
		ph.Release()
		if err != nil {
			return 0, err
		}
		pid = child
	}
}

// Count walks the leaf chain and counts entries. O(n); tests and stats only.
func (t *Tree) Count() (int, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	pid, err := t.findLeftmostLeaf()
	if err != nil {
		return 0, err
	}
	n := 0
	for pid != storage.InvalidPageID {
		h, err := t.pool.FetchRead(pid)
		if err != nil {
			return 0, err
		}
		n += int(h.Page().NumSlots())
		next := h.Page().NextPage()
		h.Release()
		pid = next
	}
	return n, nil
}

// Validate checks the tree invariants: keys sorted within every node, every
// separator key consistent with the subtree below it, all leaves at the same
// depth, and the leaf chain in key order. Tests call it after every mutation
// burst - structural bugs in a B+tree are otherwise very good at hiding.
func (t *Tree) Validate() error {
	t.mu.RLock()
	defer t.mu.RUnlock()
	root, err := t.rootID()
	if err != nil {
		return err
	}
	depth := -1
	if err := t.validateNode(root, nil, nil, 0, &depth); err != nil {
		return err
	}
	// Leaf chain must be sorted and connected.
	pid, err := t.findLeftmostLeaf()
	if err != nil {
		return err
	}
	var prevKey []byte
	var prevID storage.PageID = storage.InvalidPageID
	for pid != storage.InvalidPageID {
		h, err := t.pool.FetchRead(pid)
		if err != nil {
			return err
		}
		p := h.Page()
		if p.PrevPage() != prevID {
			h.Release()
			return fmt.Errorf("%w: leaf %v prev=%v expected %v", ErrCorruptTree, pid, p.PrevPage(), prevID)
		}
		for i := uint16(0); i < p.NumSlots(); i++ {
			e, err := p.Get(i)
			if err != nil {
				h.Release()
				return err
			}
			k := leafKey(e)
			if prevKey != nil && bytes.Compare(prevKey, k) > 0 {
				h.Release()
				return fmt.Errorf("%w: leaf chain out of order at %v", ErrCorruptTree, pid)
			}
			prevKey = append([]byte(nil), k...)
		}
		prevID = pid
		pid = p.NextPage()
		h.Release()
	}
	return nil
}

func (t *Tree) validateNode(pid storage.PageID, lo, hi []byte, level int, leafDepth *int) error {
	if level > 64 {
		return ErrCorruptTree
	}
	h, err := t.pool.FetchRead(pid)
	if err != nil {
		return err
	}
	defer h.Release()
	p := h.Page()
	if err := p.Validate(); err != nil {
		return err
	}

	var prev []byte
	for i := uint16(0); i < p.NumSlots(); i++ {
		e, err := p.Get(i)
		if err != nil {
			return err
		}
		var k []byte
		if isLeaf(p) {
			k = e // composite: key||RID, same form as a separator
		} else {
			k = internalKey(e)
		}
		if prev != nil && bytes.Compare(prev, k) > 0 {
			return fmt.Errorf("%w: node %v keys out of order", ErrCorruptTree, pid)
		}
		if lo != nil && bytes.Compare(k, lo) < 0 {
			return fmt.Errorf("%w: node %v key below its subtree bound", ErrCorruptTree, pid)
		}
		if hi != nil && bytes.Compare(k, hi) >= 0 {
			return fmt.Errorf("%w: node %v key above its subtree bound", ErrCorruptTree, pid)
		}
		prev = append([]byte(nil), k...)
	}

	if isLeaf(p) {
		if *leafDepth == -1 {
			*leafDepth = level
		} else if *leafDepth != level {
			return fmt.Errorf("%w: leaves at different depths (%d and %d)", ErrCorruptTree, *leafDepth, level)
		}
		return nil
	}

	// Recurse into every child with tightened bounds.
	prevKey := lo
	child, err := childAt(p, -1)
	if err != nil {
		return err
	}
	for i := 0; i <= int(p.NumSlots()); i++ {
		var upper []byte
		if i < int(p.NumSlots()) {
			e, err := p.Get(uint16(i))
			if err != nil {
				return err
			}
			upper = internalKey(e)
		} else {
			upper = hi
		}
		if err := t.validateNode(child, prevKey, upper, level+1, leafDepth); err != nil {
			return err
		}
		if i < int(p.NumSlots()) {
			e, _ := p.Get(uint16(i))
			prevKey = internalKey(e)
			child = internalChild(e)
		}
	}
	return nil
}
