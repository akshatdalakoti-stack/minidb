package query

import (
	"strings"
	"testing"
	"time"

	"minidb/db"
)

func newSession(t testing.TB) (*Session, *db.DB) {
	t.Helper()
	opts := db.DefaultOptions()
	opts.PoolSize = 64
	opts.LockTimeout = 3 * time.Second
	d, err := db.Open(t.TempDir(), opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { d.Close() })
	s := NewSession(d)
	t.Cleanup(func() { s.Close() })
	return s, d
}

func exec(t testing.TB, s *Session, sql string) *Result {
	t.Helper()
	res, err := s.Exec(sql)
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return res
}

func TestParserRoundTrip(t *testing.T) {
	cases := []string{
		"CREATE TABLE users (id INT, name TEXT)",
		"create table t (a int)",
		"CREATE INDEX ix ON users (id)",
		"CREATE UNIQUE INDEX ix ON users (id)",
		"INSERT INTO users VALUES (1, 'alice')",
		"INSERT INTO users (id, name) VALUES (1, 'a'), (2, 'b')",
		"SELECT * FROM users",
		"SELECT id, name FROM users WHERE id = 1 AND name != 'x' ORDER BY id DESC LIMIT 10",
		"UPDATE users SET name = 'bob' WHERE id = 1",
		"DELETE FROM users WHERE id >= 5",
		"BEGIN",
		"COMMIT",
		"ROLLBACK",
		"SHOW TABLES",
		"EXPLAIN SELECT * FROM users WHERE id = 3",
		"-- a comment\nSELECT * FROM users;",
	}
	for _, sql := range cases {
		if _, err := Parse(sql); err != nil {
			t.Errorf("%q: %v", sql, err)
		}
	}
	bad := []string{
		"SELECT",
		"SELECT * FROM",
		"INSERT INTO t VALUES 1",
		"CREATE TABLE t ()",
		"CREATE TABLE t (a nonsense)",
		"SELECT * FROM t WHERE",
		"SELECT * FROM t WHERE a == 1",
		"DELETE users",
		"SELECT * FROM t LIMIT abc",
		"SELECT * FROM t extra",
		"INSERT INTO t VALUES ('unterminated)",
	}
	for _, sql := range bad {
		if st, err := Parse(sql); err == nil {
			t.Errorf("%q parsed as %T, expected an error", sql, st)
		}
	}
}

func TestLexerStringsAndNumbers(t *testing.T) {
	st, err := Parse(`INSERT INTO t VALUES ('it''s here', -42, NULL)`)
	if err != nil {
		t.Fatal(err)
	}
	ins := st.(*Insert)
	row := ins.Rows[0]
	if row[0].Text != "it's here" {
		t.Fatalf("escaped quote: %q", row[0].Text)
	}
	if row[1].Int != -42 {
		t.Fatalf("negative number: %v", row[1].Int)
	}
	if !row[2].Null {
		t.Fatal("NULL not recognized")
	}
}

func TestEndToEndSQL(t *testing.T) {
	s, _ := newSession(t)

	exec(t, s, "CREATE TABLE users (id INT, name TEXT, age INT)")
	exec(t, s, "INSERT INTO users VALUES (1, 'alice', 30), (2, 'bob', 25), (3, 'carol', 35)")
	exec(t, s, "INSERT INTO users (id, name) VALUES (4, 'dave')")

	res := exec(t, s, "SELECT * FROM users")
	if len(res.Rows) != 4 {
		t.Fatalf("expected 4 rows, got %d\n%s", len(res.Rows), res)
	}

	res = exec(t, s, "SELECT name FROM users WHERE age > 28 ORDER BY age DESC")
	if len(res.Rows) != 2 || res.Rows[0][0].Text != "carol" || res.Rows[1][0].Text != "alice" {
		t.Fatalf("filter/order wrong:\n%s", res)
	}

	res = exec(t, s, "SELECT id FROM users ORDER BY id LIMIT 2")
	if len(res.Rows) != 2 || res.Rows[0][0].Int != 1 || res.Rows[1][0].Int != 2 {
		t.Fatalf("limit wrong:\n%s", res)
	}

	// The unlisted column came out NULL.
	res = exec(t, s, "SELECT age FROM users WHERE id = 4")
	if len(res.Rows) != 1 || !res.Rows[0][0].Null {
		t.Fatalf("expected NULL age:\n%s", res)
	}
	res = exec(t, s, "SELECT id FROM users WHERE age = NULL")
	if len(res.Rows) != 1 || res.Rows[0][0].Int != 4 {
		t.Fatalf("IS NULL matching wrong:\n%s", res)
	}

	res = exec(t, s, "UPDATE users SET age = 31 WHERE id = 1")
	if res.Affected != 1 {
		t.Fatalf("update affected %d rows", res.Affected)
	}
	res = exec(t, s, "SELECT age FROM users WHERE id = 1")
	if res.Rows[0][0].Int != 31 {
		t.Fatalf("update did not take: %v", res.Rows[0][0])
	}

	res = exec(t, s, "DELETE FROM users WHERE id >= 3")
	if res.Affected != 2 {
		t.Fatalf("delete affected %d rows", res.Affected)
	}
	res = exec(t, s, "SELECT * FROM users")
	if len(res.Rows) != 2 {
		t.Fatalf("after delete:\n%s", res)
	}

	// Type errors are reported, not silently ignored.
	if _, err := s.Exec("INSERT INTO users VALUES ('x', 'y', 'z')"); err == nil {
		t.Fatal("string into an INT column was accepted")
	}
	if _, err := s.Exec("SELECT * FROM users WHERE age = 'old'"); err == nil {
		t.Fatal("comparing an INT column with text was accepted")
	}
	if _, err := s.Exec("SELECT nope FROM users"); err == nil {
		t.Fatal("unknown column accepted")
	}
	if _, err := s.Exec("SELECT * FROM nosuchtable"); err == nil {
		t.Fatal("unknown table accepted")
	}
}

func TestIndexIsUsedWhenAvailable(t *testing.T) {
	s, _ := newSession(t)
	exec(t, s, "CREATE TABLE users (id INT, name TEXT)")
	for i := 0; i < 100; i++ {
		exec(t, s, "INSERT INTO users VALUES ("+itoa(i)+", 'n')")
	}
	// Without an index the planner has no choice.
	res := exec(t, s, "EXPLAIN SELECT * FROM users WHERE id = 42")
	if !strings.Contains(res.Message, "sequential scan") {
		t.Fatalf("expected a seq scan plan, got %q", res.Message)
	}
	exec(t, s, "CREATE INDEX users_id ON users (id)")
	res = exec(t, s, "EXPLAIN SELECT * FROM users WHERE id = 42")
	if !strings.Contains(res.Message, "index lookup") {
		t.Fatalf("index not used after CREATE INDEX: %q", res.Message)
	}
	res = exec(t, s, "EXPLAIN SELECT * FROM users WHERE id > 10 AND id < 20")
	if !strings.Contains(res.Message, "index range") {
		t.Fatalf("range not planned as an index range: %q", res.Message)
	}
	// And the answers agree with the unindexed ones.
	res = exec(t, s, "SELECT id FROM users WHERE id > 10 AND id < 20")
	if len(res.Rows) != 9 {
		t.Fatalf("index range returned %d rows, want 9:\n%s", len(res.Rows), res)
	}
	res = exec(t, s, "SELECT id FROM users WHERE id = 42")
	if len(res.Rows) != 1 || res.Rows[0][0].Int != 42 {
		t.Fatalf("index lookup wrong:\n%s", res)
	}
	// A condition on an unindexed column still works, via a scan.
	res = exec(t, s, "SELECT id FROM users WHERE name = 'n' AND id = 7")
	if len(res.Rows) != 1 {
		t.Fatalf("mixed conditions:\n%s", res)
	}
}

func TestExplicitTransactions(t *testing.T) {
	s, d := newSession(t)
	exec(t, s, "CREATE TABLE t (id INT)")
	exec(t, s, "BEGIN")
	exec(t, s, "INSERT INTO t VALUES (1)")
	exec(t, s, "INSERT INTO t VALUES (2)")
	if !s.InTransaction() {
		t.Fatal("session should be in a transaction")
	}
	exec(t, s, "ROLLBACK")

	res := exec(t, s, "SELECT * FROM t")
	if len(res.Rows) != 0 {
		t.Fatalf("rollback did not undo the inserts:\n%s", res)
	}

	exec(t, s, "BEGIN")
	exec(t, s, "INSERT INTO t VALUES (3)")
	exec(t, s, "COMMIT")
	res = exec(t, s, "SELECT * FROM t")
	if len(res.Rows) != 1 || res.Rows[0][0].Int != 3 {
		t.Fatalf("commit lost the insert:\n%s", res)
	}

	if _, err := s.Exec("COMMIT"); err == nil {
		t.Fatal("COMMIT outside a transaction was accepted")
	}
	// A second session sees the committed row.
	s2 := NewSession(d)
	defer s2.Close()
	res = exec(t, s2, "SELECT * FROM t")
	if len(res.Rows) != 1 {
		t.Fatalf("second session sees %d rows", len(res.Rows))
	}
}
