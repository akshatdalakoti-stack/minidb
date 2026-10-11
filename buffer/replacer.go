package buffer

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
