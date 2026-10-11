package buffer

import "math"

// ---------------------------------------------------------------------------
// Phase 4: page replacement policies.
//
// A Replacer answers one question: "the pool is full and I need a frame - which
// one should I throw away?". It only ever sees frame numbers; it knows nothing
// about page ids, dirtiness or pinning. The buffer pool tells it which frames
// are evictable (pin count zero); the policy picks among those.
//
// All methods are called with the buffer pool mutex held, so implementations
// need no locking of their own.
// ---------------------------------------------------------------------------

// Replacer is the page replacement policy interface.
type Replacer interface {
	// RecordAccess notes that a frame was just read or written.
	RecordAccess(frame int)
	// SetEvictable marks whether a frame is a legal victim. A pinned page is
	// never evictable - that is the buffer pool's central safety invariant.
	SetEvictable(frame int, evictable bool)
	// Evict chooses a victim, marks it non-resident and returns it.
	Evict() (int, bool)
	// Remove forgets a frame entirely (used when a page is deleted).
	Remove(frame int)
	// Size is the number of frames currently eligible for eviction.
	Size() int
	// Name identifies the policy in stats output.
	Name() string
}

// --- LRU -------------------------------------------------------------------

// LRUReplacer keeps frames in an intrusive doubly linked list ordered by
// recency of use. The list is stored in two int slices indexed by frame
// number, so a hit is O(1) with zero allocation - important, because
// RecordAccess runs on every single page fetch.
type LRUReplacer struct {
	prev, next []int
	inList     []bool
	evictable  []bool
	head, tail int // head = most recently used, tail = least recently used
	nEvictable int
}

const nilFrame = -1

// NewLRUReplacer creates a replacer for n frames.
func NewLRUReplacer(n int) *LRUReplacer {
	r := &LRUReplacer{
		prev:      make([]int, n),
		next:      make([]int, n),
		inList:    make([]bool, n),
		evictable: make([]bool, n),
		head:      nilFrame,
		tail:      nilFrame,
	}
	for i := range r.prev {
		r.prev[i], r.next[i] = nilFrame, nilFrame
	}
	return r
}

func (r *LRUReplacer) Name() string { return "LRU" }

func (r *LRUReplacer) unlink(f int) {
	if !r.inList[f] {
		return
	}
	if r.prev[f] != nilFrame {
		r.next[r.prev[f]] = r.next[f]
	} else {
		r.head = r.next[f]
	}
	if r.next[f] != nilFrame {
		r.prev[r.next[f]] = r.prev[f]
	} else {
		r.tail = r.prev[f]
	}
	r.prev[f], r.next[f] = nilFrame, nilFrame
	r.inList[f] = false
}

func (r *LRUReplacer) pushFront(f int) {
	r.prev[f] = nilFrame
	r.next[f] = r.head
	if r.head != nilFrame {
		r.prev[r.head] = f
	}
	r.head = f
	if r.tail == nilFrame {
		r.tail = f
	}
	r.inList[f] = true
}

func (r *LRUReplacer) RecordAccess(f int) {
	r.unlink(f)
	r.pushFront(f)
}

func (r *LRUReplacer) SetEvictable(f int, ev bool) {
	if !r.inList[f] && ev {
		r.pushFront(f)
	}
	if r.evictable[f] == ev {
		return
	}
	r.evictable[f] = ev
	if ev {
		r.nEvictable++
	} else {
		r.nEvictable--
	}
}

func (r *LRUReplacer) Evict() (int, bool) {
	for f := r.tail; f != nilFrame; f = r.prev[f] {
		if r.evictable[f] {
			r.Remove(f)
			return f, true
		}
	}
	return 0, false
}

func (r *LRUReplacer) Remove(f int) {
	r.unlink(f)
	if r.evictable[f] {
		r.evictable[f] = false
		r.nEvictable--
	}
}

func (r *LRUReplacer) Size() int { return r.nEvictable }

// --- Clock -----------------------------------------------------------------

// ClockReplacer is the second chance approximation of LRU: one reference bit
// per frame and a rotating hand. It trades precision for a hit path that is a
// single byte store, which is why real systems (and PostgreSQL) use a variant
// of it rather than a true LRU list.
type ClockReplacer struct {
	ref        []bool
	resident   []bool
	evictable  []bool
	hand       int
	nEvictable int
}

func NewClockReplacer(n int) *ClockReplacer {
	return &ClockReplacer{
		ref:       make([]bool, n),
		resident:  make([]bool, n),
		evictable: make([]bool, n),
	}
}

func (r *ClockReplacer) Name() string { return "Clock" }

func (r *ClockReplacer) RecordAccess(f int) {
	r.ref[f] = true
	r.resident[f] = true
}

func (r *ClockReplacer) SetEvictable(f int, ev bool) {
	r.resident[f] = true
	if r.evictable[f] == ev {
		return
	}
	r.evictable[f] = ev
	if ev {
		r.nEvictable++
	} else {
		r.nEvictable--
	}
}

func (r *ClockReplacer) Evict() (int, bool) {
	n := len(r.ref)
	if n == 0 || r.nEvictable == 0 {
		return 0, false
	}
	// Two full sweeps at most: the first clears reference bits, the second is
	// guaranteed to find a victim among the evictable frames.
	for i := 0; i < 2*n; i++ {
		f := r.hand
		r.hand = (r.hand + 1) % n
		if !r.resident[f] || !r.evictable[f] {
			continue
		}
		if r.ref[f] {
			r.ref[f] = false // second chance
			continue
		}
		r.Remove(f)
		return f, true
	}
	return 0, false
}

func (r *ClockReplacer) Remove(f int) {
	r.resident[f] = false
	r.ref[f] = false
	if r.evictable[f] {
		r.evictable[f] = false
		r.nEvictable--
	}
}

func (r *ClockReplacer) Size() int { return r.nEvictable }

// --- LRU-K -----------------------------------------------------------------

// LRUKReplacer evicts the frame with the largest backward k-distance: the
// distance to its k-th most recent access. Frames accessed fewer than k times
// have infinite distance and are evicted first (oldest first among them).
//
// Why it exists: plain LRU is fooled by a sequential scan, which touches a
// huge number of pages exactly once and flushes the genuinely hot working set
// out of the pool. LRU-K (k=2) keeps a page only if it has been referenced
// twice, so a scan cannot evict the hot set.
type LRUKReplacer struct {
	k          int
	history    [][]uint64 // per frame, the last k access timestamps (oldest first)
	resident   []bool
	evictable  []bool
	nEvictable int
	clock      uint64
}

func NewLRUKReplacer(n, k int) *LRUKReplacer {
	if k < 1 {
		k = 1
	}
	return &LRUKReplacer{
		k:         k,
		history:   make([][]uint64, n),
		resident:  make([]bool, n),
		evictable: make([]bool, n),
	}
}

func (r *LRUKReplacer) Name() string { return "LRU-K" }

func (r *LRUKReplacer) RecordAccess(f int) {
	r.clock++
	r.resident[f] = true
	h := r.history[f]
	h = append(h, r.clock)
	if len(h) > r.k {
		h = h[len(h)-r.k:]
	}
	r.history[f] = h
}

func (r *LRUKReplacer) SetEvictable(f int, ev bool) {
	r.resident[f] = true
	if r.evictable[f] == ev {
		return
	}
	r.evictable[f] = ev
	if ev {
		r.nEvictable++
	} else {
		r.nEvictable--
	}
}

func (r *LRUKReplacer) Evict() (int, bool) {
	victim := -1
	var bestDist float64 = -1
	var bestEarliest uint64 = math.MaxUint64

	for f := range r.history {
		if !r.resident[f] || !r.evictable[f] {
			continue
		}
		h := r.history[f]
		var dist float64
		if len(h) < r.k {
			dist = math.Inf(1)
		} else {
			dist = float64(r.clock - h[0])
		}
		earliest := uint64(0)
		if len(h) > 0 {
			earliest = h[0]
		}
		if dist > bestDist || (dist == bestDist && earliest < bestEarliest) {
			victim, bestDist, bestEarliest = f, dist, earliest
		}
	}
	if victim < 0 {
		return 0, false
	}
	r.Remove(victim)
	return victim, true
}

func (r *LRUKReplacer) Remove(f int) {
	r.history[f] = nil
	r.resident[f] = false
	if r.evictable[f] {
		r.evictable[f] = false
		r.nEvictable--
	}
}

func (r *LRUKReplacer) Size() int { return r.nEvictable }
