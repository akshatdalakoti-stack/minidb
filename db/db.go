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
	"minidb/pageops"
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

// Dir is the database directory.
func (d *DB) Dir() string { return d.dir }

// Pool exposes the buffer pool (stats, tests).
func (d *DB) Pool() *buffer.Pool { return d.pool }

// Log exposes the write-ahead log.
func (d *DB) Log() *wal.Manager { return d.log }

// Disk exposes the disk manager.
func (d *DB) Disk() *storage.DiskManager { return d.dm }

// Txns exposes the transaction manager.
func (d *DB) Txns() *txn.Manager { return d.txns }

// Catalog exposes the catalog.
func (d *DB) Catalog() *Catalog { return d.cat }

// Report returns what the last recovery did.
func (d *DB) Report() RecoveryReport { return d.report }

// ctx builds a logging context for a transaction.
func (d *DB) ctx(t *txn.Txn) pageops.Ctx {
	c := pageops.Ctx{Pool: d.pool, Log: d.log}
	if t != nil {
		c.Txn = t
	}
	return c
}

// Begin starts a transaction.
func (d *DB) Begin() (*txn.Txn, error) { return d.txns.Begin() }

// Commit commits a transaction, making it durable.
func (d *DB) Commit(t *txn.Txn) error { return d.txns.Commit(t) }

// Abort rolls a transaction back.
func (d *DB) Abort(t *txn.Txn) error { return d.txns.Abort(t) }

// Update runs fn inside a transaction, committing on success and rolling back
// on error or panic.
func (d *DB) Update(fn func(t *txn.Txn) error) error {
	t, err := d.Begin()
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			_ = d.Abort(t)
		}
	}()
	if err := fn(t); err != nil {
		return err
	}
	if err := d.Commit(t); err != nil {
		return err
	}
	committed = true
	return nil
}

// View runs fn inside a read-only transaction.
func (d *DB) View(fn func(t *txn.Txn) error) error {
	t, err := d.txns.BeginReadOnly()
	if err != nil {
		return err
	}
	if err := fn(t); err != nil {
		_ = d.Abort(t)
		return err
	}
	return d.Commit(t)
}

// CreateTable creates a table in its own transaction.
func (d *DB) CreateTable(name string, schema Schema) (*Table, error) {
	var out *Table
	err := d.Update(func(t *txn.Txn) error {
		if err := d.txns.Locks().Acquire(t, txn.TableKey(0), txn.LockExclusive); err != nil {
			return err
		}
		tb, err := d.cat.createTable(d.ctx(t), name, schema)
		if err != nil {
			return err
		}
		out = tb
		return nil
	})
	return out, err
}

// CreateIndex builds a secondary index on a column and back-fills it.
func (d *DB) CreateIndex(tableName, idxName, column string, unique bool) (*Table, error) {
	var out *Table
	err := d.Update(func(t *txn.Txn) error {
		tb, err := d.cat.Get(tableName)
		if err != nil {
			return err
		}
		if err := d.txns.Locks().Acquire(t, txn.TableKey(tb.Def.ID), txn.LockExclusive); err != nil {
			return err
		}
		out, err = d.cat.createIndex(d.ctx(t), tableName, idxName, column, unique)
		return err
	})
	return out, err
}

// DropTable removes a table.
func (d *DB) DropTable(name string) error {
	return d.Update(func(t *txn.Txn) error {
		tb, err := d.cat.Get(name)
		if err != nil {
			return err
		}
		if err := d.txns.Locks().Acquire(t, txn.TableKey(tb.Def.ID), txn.LockExclusive); err != nil {
			return err
		}
		return d.cat.dropTable(d.ctx(t), name)
	})
}

// Table returns a table by name.
func (d *DB) Table(name string) (*Table, error) { return d.cat.Get(name) }

// Checkpoint makes the database state on disk match the log, so that the next
// recovery has less work to do.
//
// This is a "sharp" checkpoint: it writes a begin marker, flushes every dirty
// page, then records the still-running transactions. Recovery can then start
// its analysis at the begin marker, because every page change logged before it
// is already in the data file. A fuzzy checkpoint (which does not flush, and
// instead records the dirty page table) would block writers even less; this
// one is simpler to reason about and still bounds recovery time.
func (d *DB) Checkpoint() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return os.ErrClosed
	}
	begin, err := d.log.Append(&wal.Record{Type: wal.TypeCheckpointBegin})
	if err != nil {
		return err
	}
	if err := d.pool.FlushAllPages(); err != nil {
		return err
	}
	active := d.txns.Active()
	payload, err := encodeATT(active)
	if err != nil {
		return err
	}
	if _, err := d.log.Append(&wal.Record{Type: wal.TypeCheckpointEnd, After: payload}); err != nil {
		return err
	}
	if err := d.log.Flush(0); err != nil {
		return err
	}
	if err := d.dm.SetNextTxnID(d.txns.NextID()); err != nil {
		return err
	}
	// Only now is it safe to say "recovery may start here".
	if err := d.dm.SetCheckpointLSN(begin); err != nil {
		return err
	}
	return d.dm.Sync()
}
