package query

import (
	"fmt"
	"sort"

	"minidb/db"
	"minidb/txn"
)

// Result is what a statement produced.
type Result struct {
	Columns  []string
	Rows     [][]db.Value
	Affected int
	Message  string
	Plan     string
}

// Session executes statements against a database. It holds the current
// explicit transaction, if there is one: statements run outside BEGIN get a
// transaction of their own and commit immediately (autocommit).
type Session struct {
	d  *db.DB
	tx *txn.Txn
}

// NewSession starts a session.
func NewSession(d *db.DB) *Session { return &Session{d: d} }

// InTransaction reports whether an explicit transaction is open.
func (s *Session) InTransaction() bool { return s.tx != nil }

// Close rolls back any open transaction.
func (s *Session) Close() error {
	if s.tx != nil {
		err := s.d.Abort(s.tx)
		s.tx = nil
		return err
	}
	return nil
}

// Exec parses and runs one statement.
func (s *Session) Exec(sql string) (*Result, error) {
	st, err := Parse(sql)
	if err != nil {
		return nil, fmt.Errorf("syntax error: %w", err)
	}
	return s.Run(st)
}

// Run executes an already parsed statement.
func (s *Session) Run(st Statement) (*Result, error) {
	switch v := st.(type) {
	case *Begin:
		if s.tx != nil {
			return nil, fmt.Errorf("a transaction is already open")
		}
		t, err := s.d.Begin()
		if err != nil {
			return nil, err
		}
		s.tx = t
		return &Result{Message: fmt.Sprintf("BEGIN (txn %d)", t.ID())}, nil

	case *Commit:
		if s.tx == nil {
			return nil, fmt.Errorf("no transaction is open")
		}
		t := s.tx
		s.tx = nil
		if err := s.d.Commit(t); err != nil {
			return nil, err
		}
		return &Result{Message: fmt.Sprintf("COMMIT (txn %d)", t.ID())}, nil

	case *Rollback:
		if s.tx == nil {
			return nil, fmt.Errorf("no transaction is open")
		}
		t := s.tx
		s.tx = nil
		if err := s.d.Abort(t); err != nil {
			return nil, err
		}
		return &Result{Message: fmt.Sprintf("ROLLBACK (txn %d)", t.ID())}, nil

	case *CreateTable:
		if _, err := s.d.CreateTable(v.Name, db.Schema{Columns: v.Columns}); err != nil {
			return nil, err
		}
		return &Result{Message: fmt.Sprintf("created table %s", v.Name)}, nil

	case *CreateIndex:
		if _, err := s.d.CreateIndex(v.Table, v.Name, v.Column, v.Unique); err != nil {
			return nil, err
		}
		return &Result{Message: fmt.Sprintf("created index %s on %s(%s)", v.Name, v.Table, v.Column)}, nil

	case *DropTable:
		if err := s.d.DropTable(v.Name); err != nil {
			return nil, err
		}
		return &Result{Message: fmt.Sprintf("dropped table %s", v.Name)}, nil

	case *ShowTables:
		names := s.d.Catalog().Tables()
		sort.Strings(names)
		rows := make([][]db.Value, len(names))
		for i, n := range names {
			rows[i] = []db.Value{db.TextValue(n)}
		}
		return &Result{Columns: []string{"table"}, Rows: rows}, nil

	case *Describe:
		tb, err := s.d.Table(v.Table)
		if err != nil {
			return nil, err
		}
		var rows [][]db.Value
		for _, c := range tb.Def.Schema.Columns {
			idx := ""
			if _, def, ok := tb.IndexOn(c.Name); ok {
				idx = def.Name
			}
			rows = append(rows, []db.Value{db.TextValue(c.Name), db.TextValue(c.Type.String()), db.TextValue(idx)})
		}
		return &Result{Columns: []string{"column", "type", "index"}, Rows: rows}, nil

	case *ShowStats:
		return &Result{Message: s.d.Stats().String()}, nil

	case *Checkpoint:
		if err := s.d.Checkpoint(); err != nil {
			return nil, err
		}
		return &Result{Message: "checkpoint written"}, nil

	case *Explain:
		return s.explain(v.Stmt)

	case *Insert:
		return s.withTxn(func(t *txn.Txn) (*Result, error) { return s.insert(t, v) })

	case *Select:
		return s.withTxn(func(t *txn.Txn) (*Result, error) { return s.query(t, v) })

	case *Update:
		return s.withTxn(func(t *txn.Txn) (*Result, error) { return s.update(t, v) })

	case *Delete:
		return s.withTxn(func(t *txn.Txn) (*Result, error) { return s.delete(t, v) })
	}
	return nil, fmt.Errorf("unsupported statement %T", st)
}
