# minidb — a database storage engine in Go

A miniature database storage engine, written from scratch in Go using only the
standard library. No SQLite, no BoltDB, no B-tree package, no parser generator —
every layer is implemented here.

The goal is not to build another SQL database. It is to understand how one
works underneath: how a database lays bytes out on disk, caches pages in
memory, indexes them, logs its changes, and puts itself back together after a
crash.

## What it does

```
CREATE TABLE users (id INT, name TEXT, age INT);
INSERT INTO users VALUES (1, 'alice', 30), (2, 'bob', 25);
CREATE INDEX users_age ON users (age);
SELECT name FROM users WHERE age > 28 ORDER BY age DESC;
BEGIN; DELETE FROM users WHERE age < 30; ROLLBACK;
```

Kill the process mid-transaction and restart it: committed work is still there,
uncommitted work is gone.

## What is inside

| Layer | What it does |
|---|---|
| **Disk manager** | Maps 4 KiB pages onto a single file, with a checksum on every page |
| **Slotted pages** | Variable-length records with stable record ids |
| **Buffer pool** | Caches pages in RAM with pin counts and dirty tracking |
| **Replacement policies** | LRU, Clock and LRU-K |
| **B+tree index** | Disk-backed, with splits, merges, range scans and linked leaves |
| **Heap file** | The unordered table storage records actually live in |
| **Write-ahead log** | Every change is logged before it is applied |
| **Recovery** | ARIES-style analysis, redo and undo after a crash |
| **Transactions** | Begin/commit/abort, strict 2PL locking, deadlock detection |
| **Query layer** | Hand-written SQL lexer, parser, planner and executor |

## Try it

```bash
go build ./cmd/minidb
```

```bash
./minidb -db ./mydb
```

Or run the guided tour:

```bash
./minidb -db ./demo -f examples/demo.sql
```

## Tests and benchmarks

```bash
go test -race ./...
```

```bash
go test -bench . -benchtime 300x ./bench/
```

88 tests and 30 benchmarks, including crash-recovery tests that kill the
database mid-transaction and verify what survives.

Two numbers that make the case for the whole project:

- A point lookup **through the B+tree is ~1 480× faster** than the same lookup
  without an index (6.2 µs vs 9.2 ms over 50 000 rows).
- The same workload with a **16-page buffer pool is 8.9× slower** than with a
  4 096-page one — 61 % cache hit rate versus 100 %.

## More

- [DETAILS.md](DETAILS.md) — full feature list, API examples and benchmark tables
- [DESIGN.md](DESIGN.md) — why each subsystem exists, and what happens if the
  process crashes at every critical point

## License

MIT
