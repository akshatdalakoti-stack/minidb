-- A guided tour of the engine. Run it with:
--
--   minidb -db ./demo -f examples/demo.sql -timing
--
-- Every statement here goes through the query layer, the transaction manager,
-- the buffer pool and the write-ahead log, exactly as it would from the shell.

CREATE TABLE users (id INT, name TEXT, age INT);

-- Rows land in the heap file, wherever there is room.
INSERT INTO users VALUES (1, 'alice', 30), (2, 'bob', 25), (3, 'carol', 41);
INSERT INTO users VALUES (4, 'dave', 25), (5, 'erin', 37), (6, 'frank', 52);

-- No index yet, so this can only be answered by reading every row.
EXPLAIN SELECT name FROM users WHERE age > 30;
SELECT name, age FROM users WHERE age > 30 ORDER BY age;

-- Build an index over the data that already exists: it is back-filled from
-- the heap, row by row.
CREATE INDEX users_age ON users (age);

-- Same query, different plan.
EXPLAIN SELECT name FROM users WHERE age > 30;
SELECT name, age FROM users WHERE age > 30 ORDER BY age;

-- Equality on an indexed column is a single tree descent.
EXPLAIN SELECT * FROM users WHERE age = 25;
SELECT id, name FROM users WHERE age = 25;

-- A transaction that changes its mind. The updates are applied, logged with
-- their before images, and then reversed record by record.
BEGIN;
UPDATE users SET age = 99 WHERE id = 1;
DELETE FROM users WHERE age < 30;
SELECT id, name, age FROM users ORDER BY id;
ROLLBACK;

-- Everything is back, index included.
SELECT id, name, age FROM users ORDER BY id;
SELECT name FROM users WHERE age = 25;

-- A transaction that means it.
BEGIN;
UPDATE users SET name = 'alice cooper' WHERE id = 1;
COMMIT;
SELECT name FROM users WHERE id = 1;

-- Force everything to disk and note where recovery would start next time.
CHECKPOINT;

DESCRIBE users;
SHOW STATS;
