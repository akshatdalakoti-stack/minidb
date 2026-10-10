# minidb

A simple database storage engine built in Go, just to learn how databases
work under the hood. How data is stored on disk, indexed, and recovered
after a crash.

The plan, roughly in the order I want to build it:

- fixed size pages and a disk manager over one file
- slotted pages so records can vary in size
- a buffer pool, with a few replacement policies
- a B+tree that lives on disk
- heap tables
- a write ahead log and crash recovery
- transactions and locking
- a small bit of SQL on top

Still a work in progress.
