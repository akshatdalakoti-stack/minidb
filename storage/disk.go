package storage

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"
)

// ---------------------------------------------------------------------------
// Phase 1: the DiskManager.
//
// The DiskManager owns exactly one thing: the mapping from a PageID to a byte
// range in the database file, plus the allocation of new page ids. It knows
// nothing about records, transactions or caching. Everything above it (the
// buffer pool) talks in whole pages.
//
//	page i  <->  file bytes [i*PageSize, (i+1)*PageSize)
//
// Page 0 is the meta page. It holds the magic number, the head of the free
// page list, and a few slots the layers above use to bootstrap themselves
// (catalog root page, last checkpoint LSN, next transaction id).
// ---------------------------------------------------------------------------

const (
	metaMagic   = 0x4D444231 // "MDB1"
	metaVersion = 1

	// Meta page field offsets, relative to the start of the page. They live
	// after the common page header so the meta page still carries a checksum
	// and an LSN like every other page.
	offMetaMagic      = PageHeaderSize      // uint32
	offMetaVersion    = PageHeaderSize + 4  // uint32
	offMetaFreeHead   = PageHeaderSize + 8  // uint32
	offMetaFreeCount  = PageHeaderSize + 12 // uint32
	offMetaCatalog    = PageHeaderSize + 16 // uint32
	offMetaNumPages   = PageHeaderSize + 20 // uint32 (advisory; file size wins)
	offMetaCheckpoint = PageHeaderSize + 24 // uint64
	offMetaNextTxn    = PageHeaderSize + 32 // uint64
)

// Stats counts physical I/O. The buffer pool benchmarks report these numbers:
// the whole point of a buffer pool is to make Reads grow slower than the
// number of logical page fetches.
type Stats struct {
	Reads      int64
	Writes     int64
	Allocs     int64
	Frees      int64
	Syncs      int64
	BytesRead  int64
	BytesWrit  int64
	Corruption int64
}

// DiskManager reads and writes fixed size pages in a single file.
// It is safe for concurrent use.
type DiskManager struct {
	mu   sync.Mutex
	f    *os.File
	path string

	numPages uint32 // pages currently in the file, including the meta page
	freeHead PageID
	freeCnt  uint32
	closed   bool

	// Meta values owned by higher layers but persisted here.
	catalogRoot  PageID
	checkpointLS uint64
	nextTxnID    uint64

	stats struct {
		reads      atomic.Int64
		writes     atomic.Int64
		allocs     atomic.Int64
		frees      atomic.Int64
		syncs      atomic.Int64
		bytesRead  atomic.Int64
		bytesWrit  atomic.Int64
		corruption atomic.Int64
	}
}

// OpenDiskManager opens (creating if necessary) the database file at path.
func OpenDiskManager(path string) (*DiskManager, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("storage: open %s: %w", path, err)
	}
	dm := &DiskManager{f: f, path: path, freeHead: InvalidPageID, catalogRoot: InvalidPageID}

	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	size := fi.Size()

	// A crash in the middle of extending the file can leave a partial trailing
	// page. A half page is not a page: drop it. Anything the partial page
	// contained is either unallocated or replayable from the WAL.
	if rem := size % PageSize; rem != 0 {
		size -= rem
		if err := f.Truncate(size); err != nil {
			f.Close()
			return nil, fmt.Errorf("storage: truncating partial page: %w", err)
		}
	}
	dm.numPages = uint32(size / PageSize)

	if dm.numPages == 0 {
		if err := dm.initMeta(); err != nil {
			f.Close()
			return nil, err
		}
	} else if err := dm.loadMeta(); err != nil {
		f.Close()
		return nil, err
	}
	return dm, nil
}
