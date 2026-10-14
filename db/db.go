// Package db assembles the subsystems into a database: it opens the files,
// runs recovery, owns the catalog, and exposes transactions and tables.
//
//	               Client / query layer
//	                        |
//	                db.DB (this package)
//	        ______________/ | \______________
//	       /                |                \
//	txn.Manager        table.Heap         index.Tree
//	(2PL locks)             \                /
//	       \                 \              /
//	        \             pageops (log-then-apply)
//	         \                |            |
//	        wal.Manager  buffer.Pool       |
//	                          |
//	                  storage.DiskManager
//	                          |
//	                    data.db + wal.log
package db

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"minidb/buffer"
	"minidb/storage"
	"minidb/txn"
	"minidb/wal"
)

// File names inside a database directory.
const (
	DataFileName = "data.db"
	LogFileName  = "wal.log"
)

// Options configure a database.
type Options struct {
	// PoolSize is the number of buffer pool frames (pages of RAM).
	PoolSize int
	// Policy is the page replacement policy: "lru", "clock" or "lru-k".
	Policy string
	// LockTimeout bounds how long a transaction waits for a lock.
	LockTimeout time.Duration
	// NoSync turns off fsync. Fast, and unsafe: use it for benchmarks only.
	NoSync bool
	// ReadOnly opens the database without recovery or writes.
	ReadOnly bool
}

// DefaultOptions is a reasonable configuration: 4 MiB of buffer pool.
func DefaultOptions() Options {
	return Options{PoolSize: 1024, Policy: "lru", LockTimeout: 10 * time.Second}
}

func (o *Options) fill() {
	if o.PoolSize <= 0 {
		o.PoolSize = 1024
	}
	if o.Policy == "" {
		o.Policy = "lru"
	}
	if o.LockTimeout <= 0 {
		o.LockTimeout = 10 * time.Second
	}
}

func makeReplacer(policy string, n int) (buffer.Replacer, error) {
	switch policy {
	case "lru", "":
		return buffer.NewLRUReplacer(n), nil
	case "clock":
		return buffer.NewClockReplacer(n), nil
	case "lru-k", "lruk", "lru2":
		return buffer.NewLRUKReplacer(n, 2), nil
	}
	return nil, fmt.Errorf("db: unknown replacement policy %q", policy)
}

// DB is an open database.
type DB struct {
	dir  string
	opts Options

	dm   *storage.DiskManager
	log  *wal.Manager
	pool *buffer.Pool
	txns *txn.Manager
	cat  *Catalog

	mu     sync.Mutex
	closed bool

	// RecoveryReport describes what the last Open had to repair.
	report RecoveryReport
}

// Open opens (creating if necessary) the database in the given directory and
// runs crash recovery.
func Open(dir string, opts Options) (*DB, error) {
	opts.fill()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	dm, err := storage.OpenDiskManager(filepath.Join(dir, DataFileName))
	if err != nil {
		return nil, err
	}
	lg, err := wal.Open(filepath.Join(dir, LogFileName))
	if err != nil {
		dm.Close()
		return nil, err
	}
	lg.SetNoSync(opts.NoSync)

	replacer, err := makeReplacer(opts.Policy, opts.PoolSize)
	if err != nil {
		lg.Close()
		dm.Close()
		return nil, err
	}
	pool := buffer.NewPool(opts.PoolSize, dm, replacer)

	// The write-ahead rule, installed before anything can write a page out:
	// no dirty page may reach the disk ahead of the log records describing it.
	pool.SetLogFlusher(func(lsn wal.LSN) error { return lg.Flush(lsn + 1) })

	d := &DB{dir: dir, opts: opts, dm: dm, log: lg, pool: pool}

	if err := d.recover(); err != nil {
		d.pool.FlushAllPages()
		lg.Close()
		dm.Close()
		return nil, fmt.Errorf("db: recovery failed: %w", err)
	}
	d.txns.SetUndoer(d)

	if !opts.ReadOnly {
		// Start the new run from a clean checkpoint: everything before it is
		// on disk, so the next recovery has less to do.
		if err := d.Checkpoint(); err != nil {
			d.Close()
			return nil, err
		}
	}
	return d, nil
}
