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
## Trying it

```bash
go build ./cmd/minidb
```

```bash
./minidb -db ./mydb
```

```
minidb> CREATE TABLE users (id INT, name TEXT, age INT);
created table users
minidb> INSERT INTO users VALUES (1,'alice',30), (2,'bob',25), (3,'carol',41);
inserted 3 row(s)
minidb> CREATE INDEX users_age ON users (age);
created index users_age on users(age)
minidb> EXPLAIN SELECT name FROM users WHERE age > 28;
index range scan on users_age (age in [28, +inf]) on users
  then filter: age > 28
minidb> SELECT name, age FROM users WHERE age > 28 ORDER BY age DESC;
name  | age
------+----
carol | 41
alice | 30
(2 row(s))
```

Transactions are there when you ask for them and automatic when you do not:

```
minidb> BEGIN;
minidb*> DELETE FROM users WHERE age < 30;
deleted 1 row(s)
minidb*> ROLLBACK;
minidb> SELECT * FROM users;   -- bob is back
```

Other ways in:

```bash
./minidb -db ./mydb -e "SELECT * FROM users"
```

```bash
./minidb -db ./mydb -f script.sql
```

Dot commands in the shell: `.help`, `.tables`, `.stats`, `.recovery`,
`.checkpoint`, `.timing`, `.exit`.

## Using it as a library

```go
d, err := db.Open("./mydb", db.DefaultOptions())
defer d.Close()

tb, _ := d.CreateTable("users", db.Schema{Columns: []db.Column{
    {Name: "id", Type: db.TypeInt},
    {Name: "name", Type: db.TypeText},
}})
d.CreateIndex("users", "users_id", "id", true)

err = d.Update(func(t *txn.Txn) error {         // commits, or rolls back on error
    _, err := d.Insert(t, tb, db.Row{db.IntValue(1), db.TextValue("alice")})
    return err
})

d.View(func(t *txn.Txn) error {
    rows, err := d.IndexLookup(t, tb, "id", db.IntValue(1))
    fmt.Println(rows, err)
    return err
})
```

## Tests

```bash
go test ./...
```

```bash
go test -race ./...
```

Each part got its tests before the next part was built on top of it. Besides
the normal per operation unit tests, there are the kinds of tests that actually
find storage engine bugs:

- model tests. 20,000 random operations against a slotted page compared with a
  plain `map[slot][]byte`, and 6,000 mixed B+tree inserts and deletes compared
  with a model, checking the tree's shape as it goes: keys sorted, separators
  matching their subtrees, every leaf at the same depth, leaf chain joined up
  and in order.
- corruption tests. Flip a bit on disk behind the engine's back and check the
  page gets refused instead of trusted. Cut the log in the middle of a record
  and check the tail is trimmed and the log still works.
- crash tests. `SimulateCrash()` throws away the buffer pool and the unflushed
  log tail, which is what a power cut does. The tests then check that committed
  work comes back, uncommitted work does not, the heap and the index agree
  afterwards, and recovering twice gives the same answer as recovering once.
- concurrency tests. 100 plus goroutines inserting, updating and reading at the
  same time under `-race`, then checking no pins leaked and no row went
  missing.
- one long end to end test, `TestEndToEndDefinitionOfDone`: open, create,
  insert 100,000 rows, build an index on them, do point lookups, range scans,
  updates and deletes, commit, crash, restart, recover, check everything.
