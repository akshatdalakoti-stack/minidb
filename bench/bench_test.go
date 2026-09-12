// Package bench holds the engine-level benchmarks: whole database operations
// measured end to end, as opposed to the per-subsystem benchmarks that live in
// storage, buffer, index, table and wal.
//
// Run them all:
//
//	go test -bench . -benchmem ./bench/
//
// Every benchmark reports ops/sec plus the numbers that explain it: the buffer
// pool hit ratio and the physical reads and writes each operation cost. Those
// three together are the whole story of why a database is fast or slow.
package bench

import (
	"fmt"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"

	"minidb/db"
	"minidb/storage"
	"minidb/txn"
)

func schema() db.Schema {
	return db.Schema{Columns: []db.Column{
		{Name: "id", Type: db.TypeInt},
		{Name: "name", Type: db.TypeText},
		{Name: "score", Type: db.TypeInt},
	}}
}

func row(i int) db.Row {
	return db.Row{
		db.IntValue(int64(i)),
		db.TextValue(fmt.Sprintf("name-of-record-%08d", i)),
		db.IntValue(int64(i % 1000)),
	}
}

type fixture struct {
	d    *db.DB
	tb   *db.Table
	t    testing.TB
	dir  string
	base storage.Stats // disk counters at the start of the measured phase
}

// open creates a database sized by poolPages. NoSync is on: these benchmarks
// measure the engine, not the disk's fsync latency. BenchmarkCommitDurable
// measures that separately, because it is the one number fsync dominates.
func open(t testing.TB, poolPages int, indexed bool) *fixture {
	t.Helper()
	dir := t.TempDir()
	opts := db.DefaultOptions()
	opts.PoolSize = poolPages
	opts.NoSync = true
	d, err := db.Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { d.Close() })
	tb, err := d.CreateTable("bench", schema())
	if err != nil {
		t.Fatal(err)
	}
	if indexed {
		tb, err = d.CreateIndex("bench", "bench_id", "id", false)
		if err != nil {
			t.Fatal(err)
		}
	}
	return &fixture{d: d, tb: tb, t: t, dir: dir}
}

// fill loads n rows and returns their RIDs.
func (f *fixture) fill(n int) []storage.RID {
	rids := make([]storage.RID, 0, n)
	err := f.d.Update(func(t *txn.Txn) error {
		for i := 0; i < n; i++ {
			rid, err := f.d.Insert(t, f.tb, row(i))
			if err != nil {
				return err
			}
			rids = append(rids, rid)
		}
		return nil
	})
	if err != nil {
		f.t.Fatal(err)
	}
	return rids
}

// report attaches the metrics that explain the timing. Disk counters are
// reported as deltas over the measured phase, so that loading the data does not
// show up as I/O done by the operation under test.
func (f *fixture) report(b *testing.B, ops int64) {
	bs := f.d.Pool().Stats()
	ds := f.d.Disk().Stats()
	if ops <= 0 {
		ops = int64(b.N)
	}
	b.ReportMetric(bs.HitRatio(), "hit-ratio")
	b.ReportMetric(float64(ds.Reads-f.base.Reads)/float64(ops), "disk-reads/op")
	b.ReportMetric(float64(ds.Writes-f.base.Writes)/float64(ops), "disk-writes/op")
}

// resetCounters zeroes the statistics so that the measured phase is not
// polluted by the setup phase.
func (f *fixture) resetCounters(b *testing.B) {
	f.d.Pool().ResetStats()
	f.base = f.d.Disk().Stats()
	b.ResetTimer()
}

// --- inserts ---------------------------------------------------------------

func BenchmarkInsertSequential(b *testing.B) {
	f := open(b, 1024, false)
	f.resetCounters(b)
	err := f.d.Update(func(t *txn.Txn) error {
		for i := 0; i < b.N; i++ {
			if _, err := f.d.Insert(t, f.tb, row(i)); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		b.Fatal(err)
	}
	b.StopTimer()
	f.report(b, int64(b.N))
}

func BenchmarkInsertRandomIndexed(b *testing.B) {
	// Random keys are the hard case for a B+tree: every insert lands on a
	// different leaf, so the buffer pool cannot help as much.
	f := open(b, 1024, true)
	rng := rand.New(rand.NewSource(1))
	f.resetCounters(b)
	err := f.d.Update(func(t *txn.Txn) error {
		for i := 0; i < b.N; i++ {
			if _, err := f.d.Insert(t, f.tb, row(rng.Intn(1<<30))); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		b.Fatal(err)
	}
	b.StopTimer()
	f.report(b, int64(b.N))
}

func BenchmarkInsertSequentialIndexed(b *testing.B) {
	f := open(b, 1024, true)
	f.resetCounters(b)
	err := f.d.Update(func(t *txn.Txn) error {
		for i := 0; i < b.N; i++ {
			if _, err := f.d.Insert(t, f.tb, row(i)); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		b.Fatal(err)
	}
	b.StopTimer()
	f.report(b, int64(b.N))
}

// --- point lookups: the with-index vs without-index comparison -------------

const lookupRows = 50000

func BenchmarkPointLookupWithIndex(b *testing.B) {
	f := open(b, 1024, true)
	f.fill(lookupRows)
	rng := rand.New(rand.NewSource(2))
	f.resetCounters(b)
	err := f.d.View(func(t *txn.Txn) error {
		for i := 0; i < b.N; i++ {
			rows, err := f.d.IndexLookup(t, f.tb, "id", db.IntValue(int64(rng.Intn(lookupRows))))
			if err != nil {
				return err
			}
			if len(rows) != 1 {
				return fmt.Errorf("lookup returned %d rows", len(rows))
			}
		}
		return nil
	})
	if err != nil {
		b.Fatal(err)
	}
	b.StopTimer()
	f.report(b, int64(b.N))
}

func BenchmarkPointLookupWithoutIndex(b *testing.B) {
	// Same question, no index: every lookup is a full table scan. This is the
	// number that justifies the whole of Phase 5.
	f := open(b, 1024, false)
	f.fill(lookupRows)
	rng := rand.New(rand.NewSource(2))
	f.resetCounters(b)
	err := f.d.View(func(t *txn.Txn) error {
		for i := 0; i < b.N; i++ {
			want := int64(rng.Intn(lookupRows))
			found := false
			err := f.d.Scan(t, f.tb, func(r db.ScanRow) error {
				if r.Row[0].Int == want {
					found = true
					return db.ErrStopScan
				}
				return nil
			})
			if err != nil {
				return err
			}
			if !found {
				return fmt.Errorf("row %d not found", want)
			}
		}
		return nil
	})
	if err != nil {
		b.Fatal(err)
	}
	b.StopTimer()
	f.report(b, int64(b.N))
}

// --- range scans -----------------------------------------------------------

func BenchmarkRangeScanWithIndex(b *testing.B) {
	f := open(b, 1024, true)
	f.fill(lookupRows)
	f.resetCounters(b)
	var seen int64
	err := f.d.View(func(t *txn.Txn) error {
		for i := 0; i < b.N; i++ {
			lo := int64((i * 977) % (lookupRows - 1000))
			err := f.d.IndexRange(t, f.tb, "id", db.IntValue(lo), db.IntValue(lo+999), func(db.ScanRow) error {
				seen++
				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		b.Fatal(err)
	}
	b.StopTimer()
	b.ReportMetric(float64(seen)/float64(b.N), "rows/op")
	f.report(b, int64(b.N))
}

func BenchmarkRangeScanWithoutIndex(b *testing.B) {
	f := open(b, 1024, false)
	f.fill(lookupRows)
	f.resetCounters(b)
	var seen int64
	err := f.d.View(func(t *txn.Txn) error {
		for i := 0; i < b.N; i++ {
			lo := int64((i * 977) % (lookupRows - 1000))
			err := f.d.Scan(t, f.tb, func(r db.ScanRow) error {
				if r.Row[0].Int >= lo && r.Row[0].Int <= lo+999 {
					seen++
				}
				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		b.Fatal(err)
	}
	b.StopTimer()
	b.ReportMetric(float64(seen)/float64(b.N), "rows/op")
	f.report(b, int64(b.N))
}

func BenchmarkFullTableScan(b *testing.B) {
	f := open(b, 1024, false)
	f.fill(lookupRows)
	f.resetCounters(b)
	var seen int64
	err := f.d.View(func(t *txn.Txn) error {
		for i := 0; i < b.N; i++ {
			if err := f.d.Scan(t, f.tb, func(db.ScanRow) error { seen++; return nil }); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		b.Fatal(err)
	}
	b.StopTimer()
	b.ReportMetric(float64(seen)/float64(b.N), "rows/op")
	f.report(b, int64(b.N))
}

// --- updates and deletes ---------------------------------------------------

func BenchmarkUpdate(b *testing.B) {
	f := open(b, 1024, true)
	rids := f.fill(20000)
	rng := rand.New(rand.NewSource(3))
	f.resetCounters(b)
	err := f.d.Update(func(t *txn.Txn) error {
		for i := 0; i < b.N; i++ {
			rid := rids[rng.Intn(len(rids))]
			r, err := f.d.Get(t, f.tb, rid)
			if err != nil {
				continue
			}
			r[2] = db.IntValue(int64(i))
			if _, err := f.d.UpdateRow(t, f.tb, rid, r); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		b.Fatal(err)
	}
	b.StopTimer()
	f.report(b, int64(b.N))
}

func BenchmarkDelete(b *testing.B) {
	f := open(b, 1024, true)
	// Deletes consume rows, so make sure there are always enough.
	rids := f.fill(b.N + 1000)
	f.resetCounters(b)
	err := f.d.Update(func(t *txn.Txn) error {
		for i := 0; i < b.N; i++ {
			if err := f.d.DeleteRow(t, f.tb, rids[i]); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		b.Fatal(err)
	}
	b.StopTimer()
	f.report(b, int64(b.N))
}

// --- concurrency -----------------------------------------------------------

func BenchmarkConcurrentReads(b *testing.B) {
	f := open(b, 2048, true)
	f.fill(lookupRows)
	f.resetCounters(b)
	var errs atomic.Int64
	b.RunParallel(func(pb *testing.PB) {
		rng := rand.New(rand.NewSource(time.Now().UnixNano()))
		for pb.Next() {
			err := f.d.View(func(t *txn.Txn) error {
				_, err := f.d.IndexLookup(t, f.tb, "id", db.IntValue(int64(rng.Intn(lookupRows))))
				return err
			})
			if err != nil {
				errs.Add(1)
			}
		}
	})
	b.StopTimer()
	if errs.Load() > 0 {
		b.Fatalf("%d concurrent reads failed", errs.Load())
	}
	f.report(b, int64(b.N))
}

func BenchmarkConcurrentWrites(b *testing.B) {
	f := open(b, 2048, true)
	f.resetCounters(b)
	var next atomic.Int64
	var conflicts atomic.Int64
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := int(next.Add(1))
			err := f.d.Update(func(t *txn.Txn) error {
				_, err := f.d.Insert(t, f.tb, row(i))
				return err
			})
			if err != nil {
				conflicts.Add(1)
			}
		}
	})
	b.StopTimer()
	b.ReportMetric(float64(conflicts.Load()), "conflicts")
	f.report(b, int64(b.N))
}

// --- buffer pool sizing ----------------------------------------------------
//
// The same random-read workload over a working set that does not fit in the
// small pool but does fit in the large one. The gap between these two numbers
// is the buffer pool earning its keep.

func benchPoolSize(b *testing.B, pages int) {
	f := open(b, pages, true)
	f.fill(lookupRows)
	rng := rand.New(rand.NewSource(4))
	f.resetCounters(b)
	err := f.d.View(func(t *txn.Txn) error {
		for i := 0; i < b.N; i++ {
			if _, err := f.d.IndexLookup(t, f.tb, "id", db.IntValue(int64(rng.Intn(lookupRows)))); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		b.Fatal(err)
	}
	b.StopTimer()
	f.report(b, int64(b.N))
}

func BenchmarkSmallBufferPool(b *testing.B) { benchPoolSize(b, 16) }
func BenchmarkLargeBufferPool(b *testing.B) { benchPoolSize(b, 4096) }

// --- durability ------------------------------------------------------------

// BenchmarkCommitDurable measures a commit that really waits for the disk:
// one fsync per transaction. Compare it with BenchmarkCommitNoSync to see
// exactly what durability costs.
func BenchmarkCommitDurable(b *testing.B) {
	benchCommit(b, false)
}

func BenchmarkCommitNoSync(b *testing.B) {
	benchCommit(b, true)
}

func benchCommit(b *testing.B, nosync bool) {
	dir := b.TempDir()
	opts := db.DefaultOptions()
	opts.PoolSize = 512
	opts.NoSync = nosync
	d, err := db.Open(dir, opts)
	if err != nil {
		b.Fatal(err)
	}
	defer d.Close()
	tb, err := d.CreateTable("bench", schema())
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := d.Update(func(t *txn.Txn) error {
			_, err := d.Insert(t, tb, row(i))
			return err
		})
		if err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	s := d.Stats()
	b.ReportMetric(float64(s.Log.Syncs)/float64(b.N), "fsyncs/txn")
}

// --- recovery --------------------------------------------------------------

// BenchmarkRecovery measures how long it takes to reopen a database that was
// killed with work outstanding - the number a checkpoint interval trades
// against.
func BenchmarkRecovery(b *testing.B) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		dir := b.TempDir()
		opts := db.DefaultOptions()
		opts.PoolSize = 512
		opts.NoSync = true
		d, err := db.Open(dir, opts)
		if err != nil {
			b.Fatal(err)
		}
		tb, _ := d.CreateTable("bench", schema())
		d.Update(func(t *txn.Txn) error {
			for j := 0; j < 20000; j++ {
				if _, err := d.Insert(t, tb, row(j)); err != nil {
					return err
				}
			}
			return nil
		})
		d.SimulateCrash()

		b.StartTimer()
		d2, err := db.Open(dir, opts)
		if err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
		if i == 0 {
			b.Log(d2.Report())
		}
		d2.Close()
		b.StartTimer()
	}
}
