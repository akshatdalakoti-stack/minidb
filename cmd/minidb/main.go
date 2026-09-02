// Command minidb is an interactive shell for the storage engine.
//
//	minidb -db ./mydb                       # open a REPL
//	minidb -db ./mydb -e "SELECT * FROM t"  # run one statement
//	minidb -db ./mydb -f script.sql         # run a script
//
// It exists so the engine can be driven by hand: every statement goes through
// the same query layer, transaction manager, buffer pool and WAL that the
// tests exercise.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"minidb/db"
	"minidb/query"
)

func main() {
	var (
		dir    = flag.String("db", "minidb-data", "database directory")
		pool   = flag.Int("pool", 1024, "buffer pool size, in pages (4 KiB each)")
		policy = flag.String("policy", "lru", "page replacement policy: lru, clock or lru-k")
		nosync = flag.Bool("nosync", false, "skip fsync (fast, not durable)")
		exec   = flag.String("e", "", "execute one statement and exit")
		file   = flag.String("f", "", "execute the statements in a file and exit")
		timing = flag.Bool("timing", false, "print how long each statement took")
	)
	flag.Parse()

	opts := db.DefaultOptions()
	opts.PoolSize = *pool
	opts.Policy = *policy
	opts.NoSync = *nosync

	start := time.Now()
	d, err := db.Open(*dir, opts)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
	defer d.Close()

	rep := d.Report()
	if rep.RecordsRedone > 0 || rep.RecordsUndone > 0 {
		fmt.Printf("%s\n", rep)
	}

	s := query.NewSession(d)
	defer s.Close()

	switch {
	case *exec != "":
		if err := runOne(s, *exec, *timing); err != nil {
			fmt.Fprintln(os.Stderr, "error:", err)
			os.Exit(1)
		}
	case *file != "":
		f, err := os.Open(*file)
		if err != nil {
			fmt.Fprintln(os.Stderr, "error:", err)
			os.Exit(1)
		}
		defer f.Close()
		if err := runScript(s, f, *timing); err != nil {
			fmt.Fprintln(os.Stderr, "error:", err)
			os.Exit(1)
		}
	default:
		fmt.Printf("minidb - opened %s in %s (pool %d pages, %s)\n",
			*dir, time.Since(start).Round(time.Millisecond), *pool, *policy)
		fmt.Println(`type ".help" for help, ".exit" to quit`)
		repl(s, d, *timing)
	}
}

func runOne(s *query.Session, sql string, timing bool) error {
	start := time.Now()
	res, err := s.Exec(sql)
	if err != nil {
		return err
	}
	printResult(res, start, timing)
	return nil
}

func printResult(res *query.Result, start time.Time, timing bool) {
	out := res.String()
	if out != "" {
		fmt.Println(out)
	}
	if timing {
		fmt.Printf("(%s)\n", time.Since(start).Round(time.Microsecond))
	}
}

func runScript(s *query.Session, f *os.File, timing bool) error {
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1024*1024), 1024*1024)
	var buf strings.Builder
	line := 0
	for sc.Scan() {
		line++
		text := strings.TrimSpace(sc.Text())
		if text == "" || strings.HasPrefix(text, "--") {
			continue
		}
		buf.WriteString(text)
		buf.WriteString(" ")
		if !strings.HasSuffix(text, ";") {
			continue
		}
		stmt := strings.TrimSpace(buf.String())
		buf.Reset()
		if err := runOne(s, stmt, timing); err != nil {
			return fmt.Errorf("line %d: %w", line, err)
		}
	}
	return sc.Err()
}

func repl(s *query.Session, d *db.DB, timing bool) {
	sc := bufio.NewScanner(os.Stdin)
	sc.Buffer(make([]byte, 1024*1024), 1024*1024)
	var buf strings.Builder

	prompt := func() {
		if buf.Len() > 0 {
			fmt.Print("   ...> ")
		} else if s.InTransaction() {
			fmt.Print("minidb*> ")
		} else {
			fmt.Print("minidb> ")
		}
	}
	prompt()

	for sc.Scan() {
		text := strings.TrimSpace(sc.Text())
		if buf.Len() == 0 && strings.HasPrefix(text, ".") {
			if done := command(text, s, d, &timing); done {
				return
			}
			prompt()
			continue
		}
		if text == "" {
			prompt()
			continue
		}
		buf.WriteString(text)
		buf.WriteString(" ")
		if !strings.HasSuffix(text, ";") {
			prompt()
			continue
		}
		stmt := strings.TrimSpace(buf.String())
		buf.Reset()
		start := time.Now()
		res, err := s.Exec(stmt)
		if err != nil {
			fmt.Println("error:", err)
		} else {
			printResult(res, start, timing)
		}
		prompt()
	}
}

// command handles the dot commands, which are not SQL.
func command(text string, s *query.Session, d *db.DB, timing *bool) (done bool) {
	switch strings.ToLower(strings.Fields(text)[0]) {
	case ".exit", ".quit":
		return true
	case ".help":
		fmt.Print(helpText)
	case ".stats":
		fmt.Println(d.Stats())
	case ".tables":
		if res, err := s.Exec("SHOW TABLES"); err == nil {
			fmt.Println(res)
		}
	case ".checkpoint":
		if err := d.Checkpoint(); err != nil {
			fmt.Println("error:", err)
		} else {
			fmt.Println("checkpoint written")
		}
	case ".recovery":
		fmt.Println(d.Report())
	case ".timing":
		*timing = !*timing
		fmt.Printf("timing %v\n", *timing)
	default:
		fmt.Printf("unknown command %q - try .help\n", text)
	}
	return false
}

const helpText = `
statements (end with a semicolon):
  CREATE TABLE t (a INT, b TEXT)
  CREATE [UNIQUE] INDEX ix ON t (a)
  DROP TABLE t
  INSERT INTO t [(cols)] VALUES (...), (...)
  SELECT * | cols FROM t [WHERE c op v [AND ...]] [ORDER BY c [DESC]] [LIMIT n]
  UPDATE t SET c = v [, ...] [WHERE ...]
  DELETE FROM t [WHERE ...]
  BEGIN | COMMIT | ROLLBACK
  EXPLAIN <select|update|delete>      show the access plan
  SHOW TABLES | DESCRIBE t | SHOW STATS | CHECKPOINT

notes:
  "c = NULL" is read as IS NULL; OR is not supported.
  Outside BEGIN, each statement runs in its own transaction.

commands:
  .help .tables .stats .recovery .checkpoint .timing .exit
`
