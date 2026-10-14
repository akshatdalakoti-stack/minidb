# minidb, a storage engine in Go

A small database storage engine built from the disk up with nothing but the Go
standard library. No SQLite, no key value store, no B-tree package, no parser
generator. Pages, buffer pool, B+tree, heap files, write ahead log, recovery,
transactions, locking and a small SQL front end are all in here.

It is not a finished SQL database and is not trying to be. It is the machinery
that sits under one, written so I can explain every layer: why it is there,
what it promises, and what happens if the process gets killed at the worst
possible moment.

```
                 client / minidb shell
                          |
                     query layer            parser, planner, executor
                          |
                  transaction manager       2PL locks, deadlock detection
                 +--------+--------+
                 |                 |
           table manager      write ahead log    LSN = byte offset in the log
           (heap files)            |
                 |                 |
           B+tree index            |
                 |                 |
            buffer pool -----------+           the WAL rule lives here
                 |
            disk manager                       4 KiB pages, CRC32C each
                 |
          data.db  +  wal.log
```

## What is in it

| Part | Where |
|---|---|
| fixed size pages, page ids, records, disk manager | [`storage/page.go`](storage/page.go), [`storage/record.go`](storage/record.go), [`storage/disk.go`](storage/disk.go) |
| slotted pages: variable length records, stable RIDs, compaction | [`storage/slotted.go`](storage/slotted.go) |
| buffer pool: pin counts, dirty tracking, fetch/unpin/flush | [`buffer/pool.go`](buffer/pool.go) |
| page replacement: LRU, Clock, LRU-K | [`buffer/replacer.go`](buffer/replacer.go) |
| B+tree on disk: search, insert, delete, split, merge, range scan, linked leaves | [`index/btree.go`](index/btree.go) |
| heap table: insert, get, update, delete, scan | [`table/heap.go`](table/heap.go) |
| write ahead log: records, LSNs, group commit, torn tail handling | [`wal/`](wal/), [`pageops/`](pageops/) |
| crash recovery: analysis, redo, undo, compensation records, checkpoints | [`db/recovery.go`](db/recovery.go) |
| transactions: begin/commit/abort, strict 2PL, deadlock detection | [`txn/`](txn/) |
| query layer: lexer, parser, planner, executor | [`query/`](query/) |

Why things are built this way, plus a table of what happens when the process
crashes at each point, is in [DESIGN.md](DESIGN.md).

## Layout

```
storage/    pages, slotted pages, records, RIDs, disk manager
buffer/     buffer pool, LRU / Clock / LRU-K replacers
wal/        log records, log manager, redo and undo application
pageops/    the one place where a page change gets logged and applied
index/      B+tree on disk
table/      heap file
txn/        transactions, lock manager, deadlock detection
db/         catalog, recovery, row operations, the DB object
query/      lexer, parser, planner, executor
bench/      engine level benchmarks
cmd/minidb/ the shell
```
