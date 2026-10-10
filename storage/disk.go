package storage

import (
	"encoding/binary"
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

// Path returns the database file path.
func (dm *DiskManager) Path() string { return dm.path }

func (dm *DiskManager) initMeta() error {
	var p Page
	p.Reset()
	p.SetType(PageTypeMeta)
	p.SetNextPage(InvalidPageID)
	p.SetPrevPage(InvalidPageID)
	binary.LittleEndian.PutUint32(p.Data[offMetaMagic:], metaMagic)
	binary.LittleEndian.PutUint32(p.Data[offMetaVersion:], metaVersion)
	binary.LittleEndian.PutUint32(p.Data[offMetaFreeHead:], uint32(InvalidPageID))
	binary.LittleEndian.PutUint32(p.Data[offMetaCatalog:], uint32(InvalidPageID))
	binary.LittleEndian.PutUint32(p.Data[offMetaNumPages:], 1)
	binary.LittleEndian.PutUint64(p.Data[offMetaNextTxn:], 1)
	dm.numPages = 1
	dm.nextTxnID = 1
	if err := dm.writeAt(MetaPageID, &p); err != nil {
		return err
	}
	return dm.syncLocked()
}

func (dm *DiskManager) loadMeta() error {
	var p Page
	if err := dm.readAt(MetaPageID, &p); err != nil {
		return fmt.Errorf("storage: reading meta page: %w", err)
	}
	if got := binary.LittleEndian.Uint32(p.Data[offMetaMagic:]); got != metaMagic {
		return fmt.Errorf("storage: %s is not a minidb file (magic %#x)", dm.path, got)
	}
	if v := binary.LittleEndian.Uint32(p.Data[offMetaVersion:]); v != metaVersion {
		return fmt.Errorf("storage: unsupported file format version %d", v)
	}
	dm.freeHead = PageID(binary.LittleEndian.Uint32(p.Data[offMetaFreeHead:]))
	dm.freeCnt = binary.LittleEndian.Uint32(p.Data[offMetaFreeCount:])
	dm.catalogRoot = PageID(binary.LittleEndian.Uint32(p.Data[offMetaCatalog:]))
	dm.checkpointLS = binary.LittleEndian.Uint64(p.Data[offMetaCheckpoint:])
	dm.nextTxnID = binary.LittleEndian.Uint64(p.Data[offMetaNextTxn:])
	if dm.nextTxnID == 0 {
		dm.nextTxnID = 1
	}
	return nil
}

// writeMetaLocked flushes the in-memory meta fields to page 0.
func (dm *DiskManager) writeMetaLocked() error {
	var p Page
	if err := dm.readAt(MetaPageID, &p); err != nil {
		return err
	}
	binary.LittleEndian.PutUint32(p.Data[offMetaFreeHead:], uint32(dm.freeHead))
	binary.LittleEndian.PutUint32(p.Data[offMetaFreeCount:], dm.freeCnt)
	binary.LittleEndian.PutUint32(p.Data[offMetaCatalog:], uint32(dm.catalogRoot))
	binary.LittleEndian.PutUint32(p.Data[offMetaNumPages:], dm.numPages)
	binary.LittleEndian.PutUint64(p.Data[offMetaCheckpoint:], dm.checkpointLS)
	binary.LittleEndian.PutUint64(p.Data[offMetaNextTxn:], dm.nextTxnID)
	return dm.writeAt(MetaPageID, &p)
}

// readAt/writeAt are the only two places that touch the file. They stamp and
// verify the page checksum, so a torn or rotted page can never be handed to a
// caller as if it were valid data.
func (dm *DiskManager) readAt(id PageID, p *Page) error {
	n, err := dm.f.ReadAt(p.Data[:], int64(id)*PageSize)
	dm.stats.reads.Add(1)
	dm.stats.bytesRead.Add(int64(n))
	if err != nil && n != PageSize {
		return fmt.Errorf("storage: read page %s: %w", id, err)
	}
	if err := p.VerifyChecksum(); err != nil {
		dm.stats.corruption.Add(1)
		return fmt.Errorf("storage: page %s: %w", id, err)
	}
	return nil
}
