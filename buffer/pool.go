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
