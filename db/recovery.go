package db

import (
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"
	"minidb/pageops"
	"minidb/storage"
	"minidb/txn"
	"minidb/wal"
)

// ---------------------------------------------------------------------------
// Phase 8: crash recovery, in the ARIES shape.
//
// The process died at some arbitrary instant. What is on disk is:
//
//   - a log that is intact up to the last complete record (torn tails were
//     trimmed when the log was opened), and
//   - a data file containing some subset of the page changes that log
//     describes: everything that happened to be flushed, and nothing else.
//
// Recovery makes the data file agree with the log again, in three passes:
//
//	ANALYSIS  read forward from the last checkpoint; work out which
//	          transactions were still running (the "losers") and which pages
//	          may be out of date.
//	REDO      repeat history. Replay every logged page change whose effect is
//	          not already visible on the page (page LSN < record LSN). After
//	          this pass the database is exactly as it was at the instant of
//	          the crash - including the half-finished work of the losers.
//	UNDO      roll the losers back, newest change first, writing compensation
//	          records so that a crash during recovery does not undo twice.
//
// Redoing the losers' work first, only to undo it, looks wasteful. It is what
// makes the rest simple: after REDO every data structure is physically
// consistent, so UNDO can use the same code paths as a normal rollback.
// ---------------------------------------------------------------------------

// RecoveryReport summarizes what recovery did. Open exposes it, and the tests
// assert on it.
type RecoveryReport struct {
	Ran            bool
	StartLSN       wal.LSN
	EndLSN         wal.LSN
	RecordsScanned int
	RecordsRedone  int
	RecordsUndone  int
	Losers         []uint64
	Winners        int
	Duration       time.Duration
}

func (r RecoveryReport) String() string {
	return fmt.Sprintf("recovery: scanned=%d redone=%d undone=%d losers=%v winners=%d in %s",
		r.RecordsScanned, r.RecordsRedone, r.RecordsUndone, r.Losers, r.Winners, r.Duration)
}

// recTxn is a stand-in for a transaction that is no longer running: recovery
// needs somewhere to keep the head of an undo chain while it rolls a loser
// back. It implements pageops.TxnState.
type recTxn struct {
	mu   sync.Mutex
	id   uint64
	last wal.LSN
}

func (r *recTxn) ID() uint64 { return r.id }
func (r *recTxn) LastLSN() wal.LSN {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.last
}
func (r *recTxn) SetLastLSN(l wal.LSN) {
	r.mu.Lock()
	r.last = l
	r.mu.Unlock()
}

// encodeATT / decodeATT serialize the active transaction table into a
// checkpoint record.
func encodeATT(att map[txn.ID]wal.LSN) ([]byte, error) {
	m := make(map[string]wal.LSN, len(att))
	for id, lsn := range att {
		m[fmt.Sprint(id)] = lsn
	}
	return json.Marshal(m)
}

func decodeATT(b []byte) (map[uint64]wal.LSN, error) {
	if len(b) == 0 {
		return map[uint64]wal.LSN{}, nil
	}
	var m map[string]wal.LSN
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, err
	}
	out := make(map[uint64]wal.LSN, len(m))
	for k, v := range m {
		var id uint64
		fmt.Sscan(k, &id)
		out[id] = v
	}
	return out, nil
}

// txnInfo is what analysis learns about one transaction.
type txnInfo struct {
	lastLSN   wal.LSN
	committed bool
	ended     bool
}

// recover runs the three passes and then brings the catalog and transaction
// manager online.
func (d *DB) recover() error {
	start := time.Now()
	startLSN := d.dm.CheckpointLSN()
	if startLSN == wal.InvalidLSN {
		startLSN = 0 // start of the log
	}

	att, maxTxnID, maxPage, scanned, err := d.analysis(startLSN)
	if err != nil {
		return fmt.Errorf("analysis: %w", err)
	}
	d.report = RecoveryReport{
		Ran:            true,
		StartLSN:       startLSN,
		EndLSN:         d.log.NextLSN(),
		RecordsScanned: scanned,
	}

	// The log may mention pages that the data file lost (AllocatePage extends
	// the file without an fsync). Stretch the file back out before redo.
	if maxPage != storage.InvalidPageID {
		if err := d.dm.EnsurePages(uint32(maxPage) + 1); err != nil {
			return err
		}
	}

	redone, err := d.redo(startLSN)
	if err != nil {
		return fmt.Errorf("redo: %w", err)
	}
	d.report.RecordsRedone = redone

	// The transaction manager has to exist before undo, so that rolled back
	// transactions get ids that cannot collide with the losers.
	next := d.dm.NextTxnID()
	if maxTxnID+1 > next {
		next = maxTxnID + 1
	}
	d.txns = txn.NewManager(d.log, d.opts.LockTimeout, next)

	// Now that every page is physically up to date, the catalog can be read -
	// and undo needs it, because undoing an index change is a logical
	// operation against the tree.
	ctx := pageops.Ctx{Pool: d.pool, Log: d.log}
	cat, err := loadCatalog(ctx, d.dm)
	if err != nil {
		return err
	}
	d.cat = cat

	losers := map[uint64]wal.LSN{}
	winners := 0
	for id, info := range att {
		switch {
		case info.ended:
			winners++
		case info.committed:
			// Committed but no END record: its effects are redone and it needs
			// no undo. Write the END now so the next recovery skips it.
			winners++
			d.log.Append(&wal.Record{TxnID: id, Type: wal.TypeEnd, PrevLSN: info.lastLSN})
		default:
			losers[id] = info.lastLSN
		}
	}
	d.report.Winners = winners
	for id := range losers {
		d.report.Losers = append(d.report.Losers, id)
	}
	sort.Slice(d.report.Losers, func(i, j int) bool { return d.report.Losers[i] < d.report.Losers[j] })

	if !d.opts.ReadOnly && len(losers) > 0 {
		undone, err := d.undo(losers)
		if err != nil {
			return fmt.Errorf("undo: %w", err)
		}
		d.report.RecordsUndone = undone
	}
	if err := d.log.Flush(0); err != nil {
		return err
	}
	d.report.Duration = time.Since(start)
	return nil
}
