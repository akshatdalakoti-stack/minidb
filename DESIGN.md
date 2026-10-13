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
## Write ahead log

An LSN is a byte offset in the log file. So "read the record at LSN x" is a
seek rather than a search, and comparing two LSNs compares two points in time.
Offset 0 is taken by a 16 byte file header so that 0 can mean "no LSN".

Every logged change in the engine, heap and index alike, goes through one place
(the pageops package) and does the same three things in the same order:

```
1. append a log record for the change, with the old bytes in it
2. apply the change to the page in the buffer pool
3. stamp the record's LSN into the page header
```

Step 3 is what makes redo safe to run twice. If a page's LSN is already at or
past a record, the page already has that change and replay can skip it. Having
one implementation means the heap and the B+tree cannot drift apart in how they
log things, which they would.

Redo is close to physical: records say "apply this to slot S of page P", and
replay runs all of them, so after the redo pass the file looks exactly like it
did the instant the process died. Undo is split three ways:

- heap record changes are undone from their old bytes. Strict 2PL means nobody
  else can have touched that record in the meantime.
- index changes are undone by running the opposite operation against the tree,
  not by putting bytes back. By the time a rollback happens a split may have
  moved that entry to another page, so the page id in the log is stale.
- splits, merges and heap chain links are redo only. A split is fine whoever
  caused it, and other transactions are already using it, so undoing one would
  break them.

The log tail gets trimmed on open, carefully. A crash in the middle of an
append leaves a partial record, and the scan that looks for the end of the log
reads in chunks, where a record that happens to straddle a chunk boundary looks
exactly like a torn one. Getting that wrong cuts the log at the boundary and
silently drops every commit after it, which is the bug a 300 row recovery test
found. So a failed decode only means "end of log" at the start of a chunk that
already runs to the end of the file.

Group commit came for free. Records go into an in memory tail buffer and are
only pushed out when somebody needs them durable, so ten transactions
committing at the same time share one fsync.

## B+tree

Nodes are pages, fetched through the buffer pool. That is the point of the
whole thing: the tree works on more data than fits in RAM. With a few hundred
keys per node a million key tree is three or four levels deep, and the top two
levels stay cached, so a lookup usually costs about one real read.

Keys are variable length bytes compared with bytes.Compare. The query layer
encodes ints big endian with the sign bit flipped so that byte order and number
order agree, negatives included. A node splits when the new entry does not fit,
and the split point is chosen to balance bytes rather than key counts, since
bytes are what actually fill a page.

Separator keys are key plus RID, not just the key. This came out of a failing
test with duplicate keys. In a non-unique index the same key can live in
several leaves, and a descent that compares only keys lands on an arbitrary one
of them, so entries in the earlier leaves become invisible. Adding the RID
makes the order total, so there is always exactly one leaf where an entry
belongs. A search by key alone descends with key plus page 0, slot 0, a RID
that can never exist because page 0 is the meta page.

The root page id lives in the meta page, not in a Go field. When the root
splits, which is the only way the tree gets taller and the reason it stays
balanced, one logged header update tells every reader about the new root,
including the next process to open the file.

There is a single RWMutex for the whole tree. Real engines do latch crabbing,
where you take the child's latch and drop the parent's as soon as the child is
known not to be about to split, so different parts of the tree can be written
at once. That is the better answer and it is not what is here. One tree lock is
correct, it plays nicely with the buffer pool's per frame locks, and splits and
merges stay possible to think about. The price is that index writes to one
table go one at a time. Range scans do not hold the lock: the iterator copies a
batch of entries out under a read lock and finds its place again by key when
the batch runs out, so a long scan never blocks a writer.

One thing it cannot do: a crash in the middle of a split, where the parent
update never made it, can leave the index needing a rebuild. The heap is the
real data and is always right, so the index can be dropped and built again.
Doing better needs proper nested top actions.

## Heap file

A table is a linked chain of slotted pages with the last page recorded in the
meta page. Records go wherever they fit. If you want order, that is what an
index is for. Placement tries the last page first, so rows written together end
up together and a scan stays mostly sequential, then a small fixed number of
pages that deletes have freed space in, then it grows the file.

When an update no longer fits in its page, the heap moves the record and
returns the new RID instead of leaving a forwarding pointer behind. Only the
layer above knows which indexes exist, so only it can repoint them. A
forwarding pointer would also make every later read of that row cost two page
fetches, forever, and quietly.

Chain links are logged redo only. This was another failing test. Linking a new
page onto the end used to be a normal undoable change, so a transaction that
grew the file and then aborted would unlink the page while rolling back,
cutting other transactions' committed rows out of the table. Growing the file
is not one transaction's private business. Now an aborted transaction just
leaves an empty page behind, which costs space and nothing else.
