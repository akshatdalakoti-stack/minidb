package wal

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
)

const (
	logMagic      = 0x4D444C47 // "MDLG"
	logVersion    = 1
	logHeaderSize = 16 // magic | version | reserved; keeps LSN 0 free as "none"
)

// Stats counts log activity.
type Stats struct {
	Appends      int64
	Syncs        int64
	BytesWritten int64
	Flushes      int64
}

// Manager owns the append-only log file.
//
// Records are appended into an in-memory tail buffer and only pushed to the OS
// when somebody needs them durable. That is what makes group commit work: ten
// transactions committing at the same time share one fsync, because whoever
// calls Flush first writes everybody's records.
type Manager struct {
	mu       sync.Mutex
	f        *os.File
	path     string
	buf      []byte
	bufStart LSN // file offset of buf[0]
	nextLSN  LSN // where the next record will go
	flushed  LSN // every byte below this offset is on stable storage
	noSync   bool
	closed   bool

	appends atomic.Int64
	syncs   atomic.Int64
	bytes   atomic.Int64
	flushes atomic.Int64
}

// Open opens or creates the log file. If the previous run died mid-write, the
// torn tail is detected and trimmed here, so that the log always ends on a
// record boundary.
func Open(path string) (*Manager, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("wal: open %s: %w", path, err)
	}
	m := &Manager{f: f, path: path}
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	if fi.Size() < logHeaderSize {
		var hdr [logHeaderSize]byte
		binary.LittleEndian.PutUint32(hdr[0:], logMagic)
		binary.LittleEndian.PutUint32(hdr[4:], logVersion)
		if _, err := f.WriteAt(hdr[:], 0); err != nil {
			f.Close()
			return nil, err
		}
		if err := f.Sync(); err != nil {
			f.Close()
			return nil, err
		}
		m.nextLSN, m.flushed, m.bufStart = logHeaderSize, logHeaderSize, logHeaderSize
		return m, nil
	}
	var hdr [logHeaderSize]byte
	if _, err := f.ReadAt(hdr[:], 0); err != nil {
		f.Close()
		return nil, err
	}
	if binary.LittleEndian.Uint32(hdr[0:]) != logMagic {
		f.Close()
		return nil, fmt.Errorf("wal: %s is not a minidb log", path)
	}
	end, err := m.findEnd(fi.Size())
	if err != nil {
		f.Close()
		return nil, err
	}
	if end < fi.Size() {
		// Trailing garbage: a record that was being written when the process
		// died. It was never acknowledged to anyone, so dropping it is safe -
		// and necessary, because the next append must land on a clean boundary.
		if err := f.Truncate(end); err != nil {
			f.Close()
			return nil, err
		}
	}
	m.nextLSN, m.flushed, m.bufStart = LSN(end), LSN(end), LSN(end)
	return m, nil
}

// findEnd scans the log and returns the offset just past the last intact
// record.
//
// The scan reads in chunks, so a record can straddle a chunk boundary. That
// looks exactly like a torn record, and getting the distinction wrong is
// catastrophic: the log would be truncated at the boundary and every commit
// after it silently lost. A failed decode therefore only means "end of log"
// when it happens at the very start of a chunk that already extends to the end
// of the file.
func (m *Manager) findEnd(size int64) (int64, error) {
	off := int64(logHeaderSize)
	buf := make([]byte, 0, 256*1024)
	for off < size {
		n := size - off
		if n > int64(cap(buf)) {
			n = int64(cap(buf))
		}
		buf = buf[:n]
		if _, err := m.f.ReadAt(buf, off); err != nil && err != io.EOF {
			return 0, err
		}
		consumed := 0
		grew := false
		for consumed < len(buf) {
			_, sz, err := DecodeRecord(buf[consumed:])
			if err != nil {
				if consumed > 0 {
					break // probably straddling the chunk: resume from here
				}
				if int64(len(buf)) >= size-off {
					return off, nil // genuinely torn: this is the end
				}
				buf = make([]byte, 0, cap(buf)*2) // one record is bigger than the chunk
				grew = true
				break
			}
			consumed += sz
		}
		if grew {
			continue
		}
		if consumed == 0 {
			return off, nil
		}
		off += int64(consumed)
	}
	return off, nil
}

// SetNoSync disables fsync on flush. Only for benchmarks that measure
// throughput without the storage device in the way - it gives up durability.
func (m *Manager) SetNoSync(v bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.noSync = v
}

// Append adds a record to the log and returns its LSN. The record is not
// durable until Flush covers it.
func (m *Manager) Append(r *Record) (LSN, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return InvalidLSN, os.ErrClosed
	}
	r.LSN = m.nextLSN
	before := len(m.buf)
	buf, err := r.AppendTo(m.buf)
	if err != nil {
		return InvalidLSN, err
	}
	m.buf = buf
	m.nextLSN += LSN(len(m.buf) - before)
	m.appends.Add(1)
	return r.LSN, nil
}

// writeBufferLocked hands the buffered tail to the OS (no fsync).
func (m *Manager) writeBufferLocked() error {
	if len(m.buf) == 0 {
		return nil
	}
	if _, err := m.f.WriteAt(m.buf, int64(m.bufStart)); err != nil {
		return fmt.Errorf("wal: write log: %w", err)
	}
	m.bytes.Add(int64(len(m.buf)))
	m.bufStart += LSN(len(m.buf))
	m.buf = m.buf[:0]
	return nil
}

// Flush makes every record with LSN < upto durable. Passing 0 flushes
// everything appended so far.
//
// This is the function the buffer pool calls before writing a dirty page, and
// the function a committing transaction calls before it reports success.
func (m *Manager) Flush(upto LSN) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.flushLocked(upto)
}

func (m *Manager) flushLocked(upto LSN) error {
	if m.closed {
		return os.ErrClosed
	}
	if upto == 0 {
		upto = m.nextLSN
	}
	if upto <= m.flushed {
		return nil // already durable; the common case, and it costs nothing
	}
	if err := m.writeBufferLocked(); err != nil {
		return err
	}
	if !m.noSync {
		if err := m.f.Sync(); err != nil {
			return fmt.Errorf("wal: fsync log: %w", err)
		}
		m.syncs.Add(1)
	}
	m.flushed = m.nextLSN
	m.flushes.Add(1)
	return nil
}

// FlushedLSN is the highest LSN guaranteed to be on stable storage.
func (m *Manager) FlushedLSN() LSN {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.flushed
}

// NextLSN is where the next appended record will go (i.e. the end of the log).
func (m *Manager) NextLSN() LSN {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.nextLSN
}

// Read returns the record at a given LSN. Rollback uses it to walk a
// transaction's PrevLSN chain backwards.
func (m *Manager) Read(lsn LSN) (*Record, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if lsn < logHeaderSize || lsn >= m.nextLSN {
		return nil, fmt.Errorf("wal: LSN %d out of range", lsn)
	}
	if err := m.writeBufferLocked(); err != nil {
		return nil, err
	}
	var lenBuf [4]byte
	if _, err := m.f.ReadAt(lenBuf[:], int64(lsn)); err != nil {
		return nil, err
	}
	total := int(binary.LittleEndian.Uint32(lenBuf[:])) + frameOverhead
	if total <= frameOverhead || total > MaxRecordSize {
		return nil, ErrTornRecord
	}
	buf := make([]byte, total)
	if _, err := m.f.ReadAt(buf, int64(lsn)); err != nil {
		return nil, err
	}
	rec, _, err := DecodeRecord(buf)
	return rec, err
}

// Scan calls fn for every record from `from` to the end of the log, in order.
// Returning a non-nil error from fn stops the scan.
func (m *Manager) Scan(from LSN, fn func(*Record) error) error {
	m.mu.Lock()
	if err := m.writeBufferLocked(); err != nil {
		m.mu.Unlock()
		return err
	}
	end := int64(m.nextLSN)
	m.mu.Unlock()

	if from < logHeaderSize {
		from = logHeaderSize
	}
	off := int64(from)
	buf := make([]byte, 0, 256*1024)
	for off < end {
		want := end - off
		if want > int64(cap(buf)) {
			want = int64(cap(buf))
		}
		buf = buf[:want]
		if _, err := m.f.ReadAt(buf, off); err != nil && err != io.EOF {
			return err
		}
		consumed := 0
		for {
			rec, sz, err := DecodeRecord(buf[consumed:])
			if err != nil {
				if consumed == 0 {
					if int64(len(buf)) >= end-off {
						return fmt.Errorf("wal: scan at offset %d: %w", off, err)
					}
					buf = make([]byte, 0, cap(buf)*2) // record straddles the chunk
					consumed = -1
				}
				break
			}
			if err := fn(rec); err != nil {
				return err
			}
			consumed += sz
			if consumed >= len(buf) {
				break
			}
		}
		if consumed < 0 {
			continue
		}
		off += int64(consumed)
	}
	return nil
}
