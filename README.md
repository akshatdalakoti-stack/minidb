# minidb

A simple database storage engine built in Go, just to learn how databases
work under the hood. How data is stored on disk, indexed, and recovered
after a crash.

Standard library only. No SQLite, no key value store, no B-tree package.

Done so far:

- 4 KiB pages with checksums, and a disk manager over one file
- slotted pages, so records can vary in size and still keep stable ids
- a buffer pool with pin counts, and LRU / Clock / LRU-K replacement
- a write ahead log
- a B+tree on disk, with splits, merges and range scans
- heap tables

Still to do: recovery, transactions, and some SQL to drive it with.

Tests:

```bash
go test ./...
go test -race ./...
```

Notes on why things are built this way are in DESIGN.md.
