package wal

import (
	"encoding/binary"
	"fmt"
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
