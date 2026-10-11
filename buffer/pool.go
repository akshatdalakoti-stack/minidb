// Package buffer implements Phase 3 and Phase 4: the buffer pool, the layer
// that makes the database faster than the disk it lives on.
//
// The pool owns a fixed array of frames (page sized chunks of RAM). A page
// that a caller wants is either already in a frame (a hit) or must be read in,
// possibly evicting somebody else (a miss). Two pieces of per frame state make
// this safe:
//
//	pin count - how many callers currently hold a pointer into this frame.
//	            A page with pin count > 0 can NEVER be evicted; if it were,
//	            those callers would be reading a different page's bytes.
//	dirty     - whether the frame differs from the copy on disk. A dirty page
//	            must be written out before its frame is reused, and (once the
//	            WAL exists) only after the log records describing it are durable.
package buffer

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"minidb/storage"
)

// Errors returned by the buffer pool.
var (
	ErrNoFreeFrame  = errors.New("buffer: all frames are pinned, cannot evict")
	ErrPageNotInUse = errors.New("buffer: page is not resident in the pool")
	ErrPagePinned   = errors.New("buffer: page is still pinned")
	ErrOverUnpin    = errors.New("buffer: unpin of a page that is not pinned")
)

type frame struct {
	pageID storage.PageID
	pin    int
	dirty  bool
	// seq counts dirtying events. FlushPage clears the dirty flag only if the
	// counter has not moved since it took its copy of the page, so a write
	// that races with a flush is never mistaken for already flushed.
	seq uint64
}

// PoolStats is a snapshot of buffer pool activity.
type PoolStats struct {
	Hits      int64
	Misses    int64
	Evictions int64
	Flushes   int64
	DirtyEvic int64
	Policy    string
	Frames    int
	Resident  int
	Pinned    int
}

// HitRatio is hits / (hits+misses); the single number that says whether the
// pool is doing its job.
func (s PoolStats) HitRatio() float64 {
	total := s.Hits + s.Misses
	if total == 0 {
		return 0
	}
	return float64(s.Hits) / float64(total)
}

func (s PoolStats) String() string {
	return fmt.Sprintf("policy=%s frames=%d resident=%d pinned=%d hits=%d misses=%d ratio=%.3f evict=%d(dirty=%d) flush=%d",
		s.Policy, s.Frames, s.Resident, s.Pinned, s.Hits, s.Misses, s.HitRatio(), s.Evictions, s.DirtyEvic, s.Flushes)
}

// Pool is the buffer pool. It is safe for concurrent use.
type Pool struct {
	mu       sync.Mutex
	dm       *storage.DiskManager
	pages    []storage.Page
	frames   []frame
	latches  []sync.RWMutex // per frame content latch; see FetchRead/FetchWrite
	table    map[storage.PageID]int
	freeList []int
	replacer Replacer

	// cond wakes callers that are waiting for a frame to become evictable.
	cond        *sync.Cond
	waiters     int
	waitTimeout time.Duration

	// flushLog enforces the write-ahead rule: before a dirty page reaches the
	// disk, every log record up to the page LSN must already be durable.
	// nil until the WAL is wired up (db.Open does that).
	flushLog func(lsn uint64) error

	hits      atomic.Int64
	misses    atomic.Int64
	evictions atomic.Int64
	dirtyEvic atomic.Int64
	flushes   atomic.Int64
}

// DefaultFrameWaitTimeout bounds how long a fetch waits for a frame to become
// evictable before reporting ErrNoFreeFrame.
const DefaultFrameWaitTimeout = 5 * time.Second

// SetFrameWaitTimeout changes how long a fetch waits for a free frame.
// Zero means "fail immediately", which is what the pinning tests want.
func (bp *Pool) SetFrameWaitTimeout(d time.Duration) {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	bp.waitTimeout = d
}

// NewPool creates a pool with the given number of frames and replacement
// policy. Passing a nil replacer defaults to LRU.
func NewPool(size int, dm *storage.DiskManager, r Replacer) *Pool {
	if size < 2 {
		size = 2 // one frame is not enough to split a B+tree node
	}
	if r == nil {
		r = NewLRUReplacer(size)
	}
	bp := &Pool{
		dm:          dm,
		pages:       make([]storage.Page, size),
		frames:      make([]frame, size),
		latches:     make([]sync.RWMutex, size),
		table:       make(map[storage.PageID]int, size),
		freeList:    make([]int, 0, size),
		replacer:    r,
		waitTimeout: DefaultFrameWaitTimeout,
	}
	bp.cond = sync.NewCond(&bp.mu)
	for i := 0; i < size; i++ {
		bp.frames[i].pageID = storage.InvalidPageID
		bp.freeList = append(bp.freeList, i)
	}
	return bp
}

// SetLogFlusher installs the WAL hook. It must be called before any page is
// written back, i.e. right after the WAL is opened.
func (bp *Pool) SetLogFlusher(f func(lsn uint64) error) {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	bp.flushLog = f
}

// DiskManager exposes the underlying disk manager (used for allocation stats
// and by recovery, which needs to know how many pages exist).
func (bp *Pool) DiskManager() *storage.DiskManager { return bp.dm }

// Size is the number of frames in the pool.
func (bp *Pool) Size() int { return len(bp.pages) }

// FetchPage pins page id and returns a pointer to its frame. Every successful
// FetchPage must be paired with exactly one UnpinPage, or the frame leaks and
// the pool eventually runs out of victims.
//
// The returned pointer is only valid until the matching UnpinPage.
func (bp *Pool) FetchPage(id storage.PageID) (*storage.Page, error) {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	f, err := bp.fetchLocked(id)
	if err != nil {
		return nil, err
	}
	return &bp.pages[f], nil
}

func (bp *Pool) fetchLocked(id storage.PageID) (int, error) {
	if f, ok := bp.table[id]; ok {
		bp.frames[f].pin++
		bp.replacer.RecordAccess(f)
		bp.replacer.SetEvictable(f, false)
		bp.hits.Add(1)
		return f, nil
	}
	bp.misses.Add(1)
	f, err := bp.victimLocked()
	if err != nil {
		return 0, err
	}
	// victimLocked may have released the mutex while waiting for a frame, so
	// another goroutine could have brought the very page we want into the pool
	// in the meantime. Re-check before doing a redundant read.
	if f2, ok := bp.table[id]; ok {
		bp.releaseFrameLocked(f)
		bp.frames[f2].pin++
		bp.replacer.RecordAccess(f2)
		bp.replacer.SetEvictable(f2, false)
		return f2, nil
	}
	if err := bp.dm.ReadPage(id, &bp.pages[f]); err != nil {
		bp.releaseFrameLocked(f) // give the frame back
		return 0, err
	}
	bp.frames[f] = frame{pageID: id, pin: 1}
	bp.table[id] = f
	bp.replacer.RecordAccess(f)
	bp.replacer.SetEvictable(f, false)
	return f, nil
}

// releaseFrameLocked returns an unused frame to the free list and wakes anyone
// waiting for one.
func (bp *Pool) releaseFrameLocked(f int) {
	bp.frames[f] = frame{pageID: storage.InvalidPageID}
	bp.freeList = append(bp.freeList, f)
	bp.cond.Broadcast()
}

// victimLocked returns an unused frame, evicting if necessary. The evicted
// page is written back first if dirty.
//
// If every frame is pinned there is no victim to be had right now, but a
// concurrent caller is probably about to unpin one. Rather than failing a
// perfectly legal workload (more concurrent readers than frames), we wait on a
// condition variable until a frame frees up, giving up after WaitTimeout so
// that a genuine deadlock - every frame pinned by a caller that is itself
// waiting for a frame - surfaces as an error instead of hanging forever.
func (bp *Pool) victimLocked() (int, error) {
	deadline := time.Now().Add(bp.waitTimeout)
	for {
		f, ok, err := bp.tryVictimLocked()
		if err != nil {
			return 0, err
		}
		if ok {
			return f, nil
		}
		remaining := time.Until(deadline)
		if bp.waitTimeout <= 0 || remaining <= 0 {
			return 0, ErrNoFreeFrame
		}
		// cond.Wait has no deadline of its own, so arrange a wake-up call.
		t := time.AfterFunc(remaining, func() {
			bp.mu.Lock()
			bp.cond.Broadcast()
			bp.mu.Unlock()
		})
		bp.waiters++
		bp.cond.Wait() // releases bp.mu while blocked
		bp.waiters--
		t.Stop()
	}
}

func (bp *Pool) tryVictimLocked() (int, bool, error) {
	if n := len(bp.freeList); n > 0 {
		f := bp.freeList[n-1]
		bp.freeList = bp.freeList[:n-1]
		return f, true, nil
	}
	f, ok := bp.replacer.Evict()
	if !ok {
		return 0, false, nil
	}
	old := bp.frames[f]
	// Pin count must be zero here: the replacer is only ever told a frame is
	// evictable when its pin count drops to zero, which also means nobody can
	// be holding its content latch.
	if old.pin != 0 {
		return 0, false, fmt.Errorf("buffer: internal error, evicted pinned frame %d", f)
	}
	if old.dirty {
		if err := bp.writeFrameLocked(f); err != nil {
			return 0, false, err
		}
		bp.dirtyEvic.Add(1)
	}
	delete(bp.table, old.pageID)
	bp.evictions.Add(1)
	bp.frames[f] = frame{pageID: storage.InvalidPageID}
	return f, true, nil
}

// writeFrameLocked writes frame f back to disk, honouring the WAL rule.
// Called with bp.mu held and with the frame guaranteed not to be mutating
// (either pin==0, or the caller holds the frame latch).
func (bp *Pool) writeFrameLocked(f int) error {
	id := bp.frames[f].pageID
	if bp.flushLog != nil {
		if err := bp.flushLog(bp.pages[f].LSN()); err != nil {
			return fmt.Errorf("buffer: WAL flush before page write: %w", err)
		}
	}
	if err := bp.dm.WritePage(id, &bp.pages[f]); err != nil {
		return err
	}
	bp.frames[f].dirty = false
	bp.flushes.Add(1)
	return nil
}

// UnpinPage releases one pin on page id. dirty says whether the caller
// modified the page; dirtiness is sticky (any dirty unpin marks the frame).
func (bp *Pool) UnpinPage(id storage.PageID, dirty bool) error {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	f, ok := bp.table[id]
	if !ok {
		return ErrPageNotInUse
	}
	return bp.unpinFrameLocked(f, dirty)
}

func (bp *Pool) unpinFrameLocked(f int, dirty bool) error {
	if bp.frames[f].pin <= 0 {
		return ErrOverUnpin
	}
	if dirty {
		bp.frames[f].dirty = true
		bp.frames[f].seq++
	}
	bp.frames[f].pin--
	if bp.frames[f].pin == 0 {
		bp.replacer.SetEvictable(f, true)
		bp.cond.Broadcast() // a frame just became a legal victim
	}
	return nil
}

// NewPage allocates a fresh page on disk and returns it pinned. The page comes
// back zeroed; the caller decides what to initialize it as.
func (bp *Pool) NewPage() (storage.PageID, *storage.Page, error) {
	id, err := bp.dm.AllocatePage()
	if err != nil {
		return storage.InvalidPageID, nil, err
	}
	bp.mu.Lock()
	defer bp.mu.Unlock()
	f, err := bp.victimLocked()
	if err != nil {
		return storage.InvalidPageID, nil, err
	}
	bp.pages[f].Reset()
	bp.frames[f] = frame{pageID: id, pin: 1, dirty: true}
	bp.table[id] = f
	bp.replacer.RecordAccess(f)
	bp.replacer.SetEvictable(f, false)
	return id, &bp.pages[f], nil
}

// DeletePage removes a page from the pool and returns it to the disk manager's
// free list. It fails if anybody still has the page pinned.
func (bp *Pool) DeletePage(id storage.PageID) error {
	bp.mu.Lock()
	if f, ok := bp.table[id]; ok {
		if bp.frames[f].pin != 0 {
			bp.mu.Unlock()
			return ErrPagePinned
		}
		delete(bp.table, id)
		bp.replacer.Remove(f)
		bp.releaseFrameLocked(f)
	}
	bp.mu.Unlock()
	return bp.dm.DeallocatePage(id)
}

// FlushPage writes page id back to disk if it is resident, whether or not it
// is dirty. It never blocks on a frame latch while holding the pool mutex:
// the page is pinned first, then latched, then copied, which keeps the flush
// path deadlock free with respect to writers that fetch other pages.
func (bp *Pool) FlushPage(id storage.PageID) error {
	bp.mu.Lock()
	f, ok := bp.table[id]
	if !ok {
		bp.mu.Unlock()
		return nil
	}
	bp.frames[f].pin++
	bp.replacer.SetEvictable(f, false)
	seq := bp.frames[f].seq
	bp.mu.Unlock()

	bp.latches[f].RLock()
	scratch := bp.pages[f] // 4 KiB copy taken under the content latch
	bp.latches[f].RUnlock()

	var err error
	if bp.flushLog != nil {
		err = bp.flushLog(scratch.LSN())
	}
	if err == nil {
		err = bp.dm.WritePage(id, &scratch)
	}

	bp.mu.Lock()
	if err == nil && bp.frames[f].pageID == id && bp.frames[f].seq == seq {
		bp.frames[f].dirty = false
		bp.flushes.Add(1)
	}
	bp.unpinFrameLocked(f, false)
	bp.mu.Unlock()
	return err
}
