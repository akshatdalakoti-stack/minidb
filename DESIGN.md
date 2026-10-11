# Design notes

Why each part is built the way it is, and what happens if the process dies at
a bad moment. Written mostly for future me, because I know I will forget half
of this in a month.

## Pages

Pages are 4 KiB and that never changes. A page id is just the page's position
in the file, so page 3 sits at byte 3 * 4096. Turning an id into a file offset
is a multiply instead of a lookup, and the file ends up being one long array of
pages, which is easy to reason about.

4 KiB is also the size the OS and the disk like to work in, so one page write
is close to atomic in practice. Close, not actually atomic, which is the whole
reason for the checksum below.

Every page starts with the same 32 byte header, no matter what the page holds:

```
 0  LSN       uint64   how up to date this page is
 8  Type      uint8    meta / heap / btree leaf / btree internal / free
 9  Flags     uint8
10  NumSlots  uint16
12  FreeEnd   uint16   where the record area starts, it grows downward
16  Next      uint32   heap chain, leaf sibling, free list
20  Prev      uint32
24  Extra     uint32   depends on page type (btree leftmost child, heap head)
28  Checksum  uint32   CRC32C over the page with this field zeroed
```

The log and the buffer pool both need to read and stamp the LSN of any page
without knowing what kind of page it is. One shared header gives them that,
and it costs 32 bytes out of 4096.

A page read from disk has to pass its checksum or it is refused with
ErrCorruptPage. I did not want that error quietly ignored anywhere. A bad
checksum means either bit rot or a half finished write, and both are things you
want to hear about instead of reading junk and carrying on.

Growing the file does not fsync, so a crash can leave a partial page at the
end. On open the disk manager cuts the file back to a whole number of pages,
because half a page is not a page. Recovery then stretches it back out to cover
every page the log mentions. The log is the real story, the data file is just a
cache of what the log already said.

Free pages are kept in a linked list that runs through the pages themselves,
using Next in the header. Taking a page off the list is made durable right
away, because a stale list head after a crash would hand the same page out
twice while real data is still sitting in it. Putting a page back is not made
durable, since losing that only leaks a page. Wasted space is annoying, handing
out a live page is not something you recover from.

## Slotted pages

A record is found by slot number, not by its offset in the page.

```
+----------------------------------------------------+
| header | slot0 slot1 slot2 -> free <- rec2 rec1 rec0 |
+----------------------------------------------------+
```

A slot holds an offset and a length. A RID is a page id plus a slot number.
Indexes store RIDs, so the page is free to shuffle the actual bytes around,
which is what compaction does, without breaking a single index entry. Deleting
leaves the slot there as a tombstone for the same reason.

There are two kinds of page sharing one layout. A heap page wants slot numbers
to stay put, so insert and delete keep tombstones. A B+tree node wants slot
order to be key order, so its insert and delete shift the 4 byte slot entries
along. Two behaviours, one page format, one set of log record types with a flag
to say which.

There is also a pair of helpers, ReserveSlot and CanUpdate, that exist because
of a test that kept failing. Heap.Update used to try the update and then fall
back to moving the record when the page was full, but by then the log record
was already written, and recovery would later try to replay something that does
not fit. Now every operation answers "would this work?" before anything is
logged, and it answers using only the logical contents of the page, so the
answer is the same at replay time.
## Buffer pool

Pages are pinned, not garbage collected. FetchPage pins a page, UnpinPage lets
it go, and a page with a pin count above zero is never evicted, because callers
are holding real pointers into the frame. Every fetch has to be matched by one
unpin, and the tests check that nothing is still pinned at the end, which has
caught me more than once.

FetchRead and FetchWrite hand back a handle that holds both the pin and the
frame's RWMutex. Without that lock a checkpoint could copy a page to disk in
the middle of someone else's update and write out a half new, half old page
that still has a valid checksum. That is the worst kind of corruption, the kind
nothing complains about.

The flush path pins the frame while holding the pool mutex, drops the pool
mutex, and only then takes the page lock. Waiting on a page lock while still
holding the pool mutex would deadlock against any writer that needs to fetch a
second page. The dirty flag is only cleared if a version counter has not moved,
so a write that raced with the copy is not mistaken for one that made it to
disk.

If every frame is busy, a fetch waits on a condition variable instead of
failing. Sixteen goroutines on an eight frame pool is a normal workload, not an
error. There is a timeout behind that, so a real deadlock, where every frame is
pinned by someone who is also waiting, shows up as ErrNoFreeFrame instead of
hanging forever.

The one rule here: a dirty page only goes to disk after the log records that
describe it are durable. The pool calls wal.Flush(pageLSN+1) before every
write. That is write ahead logging, and it happens in exactly one place so I
cannot forget it somewhere else.

## Page replacement

Three policies behind one interface, picked when the database is opened.

LRU is an intrusive doubly linked list stored in two int slices, so a hit is
O(1) and allocates nothing. That matters because it runs on every single page
fetch.

Clock keeps one reference bit per frame and a hand that sweeps around. A hit is
a single byte write. Postgres uses this, for that reason.

LRU-K with k=2 evicts on backward k-distance. Plain LRU gets wrecked by a big
sequential scan, which touches a million pages once each and pushes out the
pages you actually care about. LRU-2 only keeps a page once it has been asked
for twice, so a scan cannot do that.

The replacer only ever sees frame numbers. It has no idea what a page id is.
