# minidb

A small database storage engine written in Go, built from the disk layer up. I
wrote it to find out how a database actually keeps data on disk, how an index
makes a lookup fast, and how it gets itself back to a sane state after a crash.

Standard library only. No SQLite, no key value store, no B-tree package, no
parser generator.

What is in it:

- 4 KiB pages with checksums, and a disk manager over one file
- slotted pages, so records can vary in size and still keep stable ids
- a buffer pool with pin counts, and LRU / Clock / LRU-K replacement
- a B+tree on disk, with splits, merges and range scans
- heap tables
- a write ahead log, and crash recovery with redo and undo
- transactions with 2PL locking and deadlock detection
- a small SQL front end and a shell

Build and run:

```bash
go build ./cmd/minidb
./minidb -db ./mydb
```

```
minidb> CREATE TABLE users (id INT, name TEXT, age INT);
minidb> INSERT INTO users VALUES (1,'alice',30), (2,'bob',25);
minidb> CREATE INDEX users_age ON users (age);
minidb> SELECT name, age FROM users WHERE age > 28;
```

Tests:

```bash
go test ./...
go test -race ./...
```

More on what is in here and how fast it is: DETAILS.md. Why it is built this
way, and what happens if the process dies at a bad moment: DESIGN.md.

Still a work in progress.
