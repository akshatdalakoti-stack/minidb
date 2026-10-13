// Package table implements Phase 6: the heap file, the unordered collection of
// records that a table actually is.
//
// A heap file is a singly linked chain of slotted pages:
//
//	meta.Extra -> page0 -> page1 -> page2 -> ... -> meta.Next (tail)
//
// Records go wherever there is room; nothing about their order is promised.
// That is deliberate: order is what indexes are for. A heap gives you O(1)
// access by RID and a cheap sequential scan, and it never has to move a record
// to keep a structure balanced.
//
// The one thing the heap must guarantee is that a RID stays valid for the life
// of the record, because that is what every index entry stores. The slotted
// page layer provides half of that (slot numbers survive compaction); the heap
// provides the other half by never silently relocating a record - an update
// that no longer fits reports its new RID to the caller instead.
package table

import (
	"errors"
	"fmt"
	"sync"

	"minidb/buffer"
	"minidb/pageops"
	"minidb/storage"
)

// Errors returned by the heap.
var (
	ErrRecordNotFound = errors.New("table: no such record")
	ErrRecordTooLarge = storage.ErrRecordTooLarge
)

// Heap is a heap file.
type Heap struct {
	pool *buffer.Pool
	meta storage.PageID
	name string
	id   uint32

	mu    sync.Mutex
	first storage.PageID
	tail  storage.PageID
	// freeish is a small set of pages that deletes have left room in. It is a
	// hint, not a source of truth: a stale entry only costs one wasted page
	// fetch. Real engines keep an on-disk free space map; this keeps the same
	// idea in memory and bounded.
	freeish   []storage.PageID
	inFreeish map[storage.PageID]bool
}

const maxFreeish = 64

// Create builds a new empty heap file and returns it.
func Create(ctx pageops.Ctx, name string, id uint32) (*Heap, error) {
	metaH, err := ctx.NewPage(storage.PageTypeMeta)
	if err != nil {
		return nil, err
	}
	defer metaH.Release()

	firstH, err := ctx.NewPage(storage.PageTypeHeap)
	if err != nil {
		return nil, err
	}
	first := firstH.ID()
	firstH.Release()

	if err := ctx.SetExtra(metaH, uint32(first)); err != nil {
		return nil, err
	}
	if err := ctx.SetNext(metaH, first); err != nil { // meta.Next tracks the tail
		return nil, err
	}
	return &Heap{
		pool: ctx.Pool, meta: metaH.ID(), name: name, id: id,
		first: first, tail: first, inFreeish: map[storage.PageID]bool{},
	}, nil
}

// Open attaches to an existing heap file.
func Open(pool *buffer.Pool, meta storage.PageID, name string, id uint32) (*Heap, error) {
	h, err := pool.FetchRead(meta)
	if err != nil {
		return nil, err
	}
	defer h.Release()
	return &Heap{
		pool: pool, meta: meta, name: name, id: id,
		first:     storage.PageID(h.Page().Extra()),
		tail:      h.Page().NextPage(),
		inFreeish: map[storage.PageID]bool{},
	}, nil
}

// MetaPage identifies the heap file on disk.
func (hp *Heap) MetaPage() storage.PageID { return hp.meta }

// Name returns the table name.
func (hp *Heap) Name() string { return hp.name }

// ID returns the table id used for locking.
func (hp *Heap) ID() uint32 { return hp.id }

// FirstPage is the head of the page chain.
func (hp *Heap) FirstPage() storage.PageID {
	hp.mu.Lock()
	defer hp.mu.Unlock()
	return hp.first
}

// Insert stores a record and returns its RID.
//
// Placement policy: try the tail page (records arriving together end up
// together, which makes scans sequential), then any page a delete has freed
// space in, then grow the file.
func (hp *Heap) Insert(ctx pageops.Ctx, rec storage.Record) (storage.RID, error) {
	if len(rec) > storage.MaxRecordSize {
		return storage.InvalidRID, fmt.Errorf("%w: %d bytes", ErrRecordTooLarge, len(rec))
	}
	// Two goroutines that both find the tail full must not both grow the file:
	// the loser would link its new page over the winner's, orphaning it. The
	// grow path is serialized and re-checked, and the retry then targets the
	// tail whoever won installed.
	for attempt := 0; attempt < 8; attempt++ {
		hp.mu.Lock()
		tried := hp.tail
		candidates := append([]storage.PageID{tried}, hp.freeish...)
		hp.mu.Unlock()

		for _, pid := range candidates {
			rid, ok, err := hp.tryInsert(ctx, pid, rec)
			if err != nil {
				return storage.InvalidRID, err
			}
			if ok {
				return rid, nil
			}
			if pid != tried {
				hp.dropFreeish(pid)
			}
		}
		rid, ok, err := hp.grow(ctx, rec, tried)
		if err != nil {
			return storage.InvalidRID, err
		}
		if ok {
			return rid, nil
		}
		// Somebody else grew the heap; go round again and use their new tail.
	}
	return storage.InvalidRID, errors.New("table: could not place record after 8 attempts")
}
