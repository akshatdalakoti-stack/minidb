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
