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

func (hp *Heap) tryInsert(ctx pageops.Ctx, pid storage.PageID, rec storage.Record) (storage.RID, bool, error) {
	if pid == storage.InvalidPageID {
		return storage.InvalidRID, false, nil
	}
	h, err := hp.pool.FetchWrite(pid)
	if err != nil {
		return storage.InvalidRID, false, err
	}
	defer h.Release()
	if h.Page().FreeSpace()+h.Page().DeadSpace() < len(rec)+storage.SlotSize {
		return storage.InvalidRID, false, nil
	}
	slot, err := ctx.Insert(h, rec)
	if err != nil {
		if errors.Is(err, storage.ErrPageFull) {
			return storage.InvalidRID, false, nil
		}
		return storage.InvalidRID, false, err
	}
	return storage.RID{PageID: pid, Slot: slot}, true, nil
}

// grow adds a page to the end of the chain and puts the record on it. It
// reports ok=false if another goroutine already grew the heap since the caller
// last looked, in which case the caller should retry against the new tail.
//
// hp.mu is held for the whole operation. Nothing takes hp.mu while holding a
// page latch, so this cannot invert a lock order.
func (hp *Heap) grow(ctx pageops.Ctx, rec storage.Record, triedTail storage.PageID) (storage.RID, bool, error) {
	hp.mu.Lock()
	defer hp.mu.Unlock()
	if hp.tail != triedTail {
		return storage.InvalidRID, false, nil
	}
	oldTail := hp.tail

	newH, err := ctx.NewPage(storage.PageTypeHeap)
	if err != nil {
		return storage.InvalidRID, false, err
	}
	defer newH.Release()
	slot, err := ctx.Insert(newH, rec)
	if err != nil {
		return storage.InvalidRID, false, err
	}

	// Link it in: tail.Next = new, then meta.Next = new. The order matters -
	// if we crash between the two, the chain is still walkable from the head
	// and only the tail hint is stale.
	//
	// The linking is logged as structural (redo-only), exactly like a B+tree
	// split. Growing the file is not this transaction's private business: once
	// the page is in the chain other transactions put their own records on it,
	// and undoing the link during a rollback would cut their committed rows
	// out of the table. An aborted transaction leaves an empty page behind
	// instead, which is only wasted space.
	sctx := ctx.Structural()
	tailH, err := hp.pool.FetchWrite(oldTail)
	if err != nil {
		return storage.InvalidRID, false, err
	}
	err = sctx.SetNext(tailH, newH.ID())
	tailH.Release()
	if err != nil {
		return storage.InvalidRID, false, err
	}
	metaH, err := hp.pool.FetchWrite(hp.meta)
	if err != nil {
		return storage.InvalidRID, false, err
	}
	err = sctx.SetNext(metaH, newH.ID())
	metaH.Release()
	if err != nil {
		return storage.InvalidRID, false, err
	}

	hp.tail = newH.ID()
	return storage.RID{PageID: newH.ID(), Slot: slot}, true, nil
}

// Get returns a copy of the record at rid.
func (hp *Heap) Get(rid storage.RID) (storage.Record, error) {
	h, err := hp.pool.FetchRead(rid.PageID)
	if err != nil {
		return nil, err
	}
	defer h.Release()
	rec, err := h.Page().Get(rid.Slot)
	if err != nil {
		if errors.Is(err, storage.ErrRecordDeleted) || errors.Is(err, storage.ErrInvalidSlot) {
			return nil, fmt.Errorf("%w: %v", ErrRecordNotFound, rid)
		}
		return nil, err
	}
	// The page is unpinned when this function returns, so the caller gets a
	// copy rather than a pointer into the frame.
	return append(storage.Record(nil), rec...), nil
}

// Update replaces the record at rid.
//
// If the new version still fits on its page, the RID does not change. If it
// does not fit, the record is moved to another page and the new RID is
// returned: the caller (which owns the indexes) is responsible for repointing
// index entries. Making relocation explicit rather than hiding it behind a
// forwarding pointer keeps the heap simple and the cost visible.
func (hp *Heap) Update(ctx pageops.Ctx, rid storage.RID, rec storage.Record) (storage.RID, error) {
	if len(rec) > storage.MaxRecordSize {
		return storage.InvalidRID, fmt.Errorf("%w: %d bytes", ErrRecordTooLarge, len(rec))
	}
	h, err := hp.pool.FetchWrite(rid.PageID)
	if err != nil {
		return storage.InvalidRID, err
	}
	if !h.Page().SlotOccupied(rid.Slot) {
		h.Release()
		return storage.InvalidRID, fmt.Errorf("%w: %v", ErrRecordNotFound, rid)
	}
	err = ctx.Update(h, rid.Slot, rec)
	if err == nil {
		h.Release()
		return rid, nil
	}
	if !errors.Is(err, storage.ErrPageFull) {
		h.Release()
		return storage.InvalidRID, err
	}
	// Does not fit any more: delete here, insert elsewhere.
	if err := ctx.Delete(h, rid.Slot); err != nil {
		h.Release()
		return storage.InvalidRID, err
	}
	h.Release()
	hp.addFreeish(rid.PageID)
	return hp.Insert(ctx, rec)
}

// Delete removes the record at rid.
func (hp *Heap) Delete(ctx pageops.Ctx, rid storage.RID) error {
	h, err := hp.pool.FetchWrite(rid.PageID)
	if err != nil {
		return err
	}
	if !h.Page().SlotOccupied(rid.Slot) {
		h.Release()
		return fmt.Errorf("%w: %v", ErrRecordNotFound, rid)
	}
	err = ctx.Delete(h, rid.Slot)
	h.Release()
	if err == nil {
		hp.addFreeish(rid.PageID)
	}
	return err
}

func (hp *Heap) addFreeish(pid storage.PageID) {
	hp.mu.Lock()
	defer hp.mu.Unlock()
	if hp.inFreeish[pid] || pid == hp.tail {
		return
	}
	if len(hp.freeish) >= maxFreeish {
		return
	}
	hp.freeish = append(hp.freeish, pid)
	hp.inFreeish[pid] = true
}

func (hp *Heap) dropFreeish(pid storage.PageID) {
	hp.mu.Lock()
	defer hp.mu.Unlock()
	if !hp.inFreeish[pid] {
		return
	}
	delete(hp.inFreeish, pid)
	for i, p := range hp.freeish {
		if p == pid {
			hp.freeish = append(hp.freeish[:i], hp.freeish[i+1:]...)
			return
		}
	}
}

// --- scanning --------------------------------------------------------------

// Row is one record produced by a scan.
type Row struct {
	RID    storage.RID
	Record storage.Record
}

// Scanner walks every live record in the heap, page by page.
//
// Like the index iterator, it copies a page's worth of records out under a
// read latch and then releases the page, so a long scan never pins a frame or
// blocks a writer for longer than one page takes to copy.
type Scanner struct {
	heap  *Heap
	next  storage.PageID
	batch []Row
	pos   int
	err   error
}

// Scan returns a scanner positioned before the first record.
func (hp *Heap) Scan() *Scanner {
	hp.mu.Lock()
	first := hp.first
	hp.mu.Unlock()
	return &Scanner{heap: hp, next: first}
}

// Next advances to the next record.
func (s *Scanner) Next() bool {
	for {
		if s.pos < len(s.batch) {
			s.pos++
			return true
		}
		if s.next == storage.InvalidPageID {
			return false
		}
		if err := s.fill(); err != nil {
			s.err = err
			return false
		}
	}
}

func (s *Scanner) fill() error {
	s.batch = s.batch[:0]
	s.pos = 0
	for s.next != storage.InvalidPageID && len(s.batch) == 0 {
		h, err := s.heap.pool.FetchRead(s.next)
		if err != nil {
			return err
		}
		p := h.Page()
		pid := s.next
		for i := uint16(0); i < p.NumSlots(); i++ {
			rec, err := p.Get(i)
			if err != nil {
				continue // tombstone
			}
			s.batch = append(s.batch, Row{
				RID:    storage.RID{PageID: pid, Slot: i},
				Record: append(storage.Record(nil), rec...),
			})
		}
		s.next = p.NextPage()
		h.Release()
	}
	return nil
}

// Row returns the current row. Valid after Next returned true.
func (s *Scanner) Row() Row { return s.batch[s.pos-1] }

// Err returns the error that stopped the scan.
func (s *Scanner) Err() error { return s.err }

// --- statistics ------------------------------------------------------------

// Stats describes the physical shape of the heap.
type Stats struct {
	Pages     int
	Records   int
	LiveBytes int
	FreeBytes int
}

// Fill is the fraction of the heap's pages occupied by live records.
func (s Stats) Fill() float64 {
	if s.Pages == 0 {
		return 0
	}
	return float64(s.LiveBytes) / float64(s.Pages*storage.PageSize)
}

// Stats walks the page chain and measures it. O(pages).
func (hp *Heap) Stats() (Stats, error) {
	var st Stats
	hp.mu.Lock()
	pid := hp.first
	hp.mu.Unlock()
	for pid != storage.InvalidPageID {
		h, err := hp.pool.FetchRead(pid)
		if err != nil {
			return st, err
		}
		p := h.Page()
		st.Pages++
		st.FreeBytes += p.FreeSpace() + p.DeadSpace()
		for i := uint16(0); i < p.NumSlots(); i++ {
			if rec, err := p.Get(i); err == nil {
				st.Records++
				st.LiveBytes += len(rec)
			}
		}
		pid = p.NextPage()
		h.Release()
	}
	return st, nil
}

// Count returns the number of live records. O(pages).
func (hp *Heap) Count() (int, error) {
	st, err := hp.Stats()
	return st.Records, err
}
