package query

import (
	"fmt"
	"sort"
	"strings"

	"minidb/db"
	"minidb/storage"
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

// withTxn runs fn in the session's transaction, or in a fresh autocommit one.
func (s *Session) withTxn(fn func(*txn.Txn) (*Result, error)) (*Result, error) {
	if s.tx != nil {
		res, err := fn(s.tx)
		if err != nil {
			// The statement failed inside an explicit transaction. Leave the
			// transaction open: it is the user's decision whether to retry or
			// roll back - except when the engine already doomed it.
			if s.tx.Aborting() {
				t := s.tx
				s.tx = nil
				s.d.Abort(t)
				return nil, fmt.Errorf("%w (transaction rolled back)", err)
			}
		}
		return res, err
	}
	t, err := s.d.Begin()
	if err != nil {
		return nil, err
	}
	res, err := fn(t)
	if err != nil {
		s.d.Abort(t)
		return nil, err
	}
	if err := s.d.Commit(t); err != nil {
		return nil, err
	}
	return res, nil
}

// --- planning --------------------------------------------------------------

// plan describes how a statement will find its rows. This is the entire query
// optimizer: pick an index if one of the conditions can use it, otherwise scan.
type plan struct {
	kind    string // "seq scan", "index lookup", "index range"
	column  string
	index   string
	eq      db.Value
	lo, hi  db.Value
	hasLo   bool
	hasHi   bool
	filters []Condition
}

func (p plan) String() string {
	switch p.kind {
	case "index lookup":
		return fmt.Sprintf("index lookup on %s (%s = %s)", p.index, p.column, p.eq)
	case "index range":
		lo, hi := "-inf", "+inf"
		if p.hasLo {
			lo = p.lo.String()
		}
		if p.hasHi {
			hi = p.hi.String()
		}
		return fmt.Sprintf("index range scan on %s (%s in [%s, %s])", p.index, p.column, lo, hi)
	}
	return "sequential scan (no usable index)"
}

// choosePlan looks for a condition that an index can answer.
func choosePlan(tb *db.Table, where []Condition) plan {
	p := plan{kind: "seq scan", filters: where}
	for _, c := range where {
		_, def, ok := tb.IndexOn(c.Column)
		if !ok {
			continue
		}
		switch c.Op {
		case "=":
			// Equality beats everything: one descent, no scan.
			return plan{kind: "index lookup", column: c.Column, index: def.Name, eq: c.Value, filters: where}
		case "<", "<=", ">", ">=":
			if p.kind == "seq scan" {
				p = plan{kind: "index range", column: c.Column, index: def.Name, filters: where}
			}
			if p.column != c.Column {
				continue
			}
			// Both bounds on the same column collapse into one range.
			switch c.Op {
			case ">", ">=":
				p.lo, p.hasLo = c.Value, true
			case "<", "<=":
				p.hi, p.hasHi = c.Value, true
			}
		}
	}
	return p
}

func (s *Session) explain(st Statement) (*Result, error) {
	var tableName string
	var where []Condition
	switch v := st.(type) {
	case *Select:
		tableName, where = v.Table, v.Where
	case *Update:
		tableName, where = v.Table, v.Where
	case *Delete:
		tableName, where = v.Table, v.Where
	default:
		return &Result{Message: "no plan: this statement does not read rows"}, nil
	}
	tb, err := s.d.Table(tableName)
	if err != nil {
		return nil, err
	}
	p := choosePlan(tb, where)
	msg := fmt.Sprintf("%s on %s", p.String(), tableName)
	if len(where) > 0 {
		msg += fmt.Sprintf("\n  then filter: %s", describeConditions(where))
	}
	return &Result{Message: msg, Plan: p.String()}, nil
}

func describeConditions(cs []Condition) string {
	parts := make([]string, len(cs))
	for i, c := range cs {
		parts[i] = fmt.Sprintf("%s %s %s", c.Column, c.Op, c.Value)
	}
	return strings.Join(parts, " AND ")
}

// --- execution -------------------------------------------------------------

// rowsFor gathers the rows a statement should consider, using the chosen plan.
func (s *Session) rowsFor(t *txn.Txn, tb *db.Table, where []Condition) ([]db.ScanRow, plan, error) {
	if err := checkConditions(tb, where); err != nil {
		return nil, plan{}, err
	}
	p := choosePlan(tb, where)
	var candidates []db.ScanRow

	switch p.kind {
	case "index lookup":
		rows, err := s.d.IndexLookup(t, tb, p.column, p.eq)
		if err != nil {
			return nil, p, err
		}
		candidates = rows
	case "index range":
		lo, hi := db.Value{Null: true}, db.Value{Null: true}
		if p.hasLo {
			lo = p.lo
		}
		if p.hasHi {
			hi = p.hi
		}
		err := s.d.IndexRange(t, tb, p.column, lo, hi, func(r db.ScanRow) error {
			candidates = append(candidates, r)
			return nil
		})
		if err != nil {
			return nil, p, err
		}
	default:
		err := s.d.Scan(t, tb, func(r db.ScanRow) error {
			candidates = append(candidates, r)
			return nil
		})
		if err != nil {
			return nil, p, err
		}
	}

	// The index narrowed things down; every condition still has to be checked,
	// because an index range is inclusive at its edges and other columns are
	// not covered at all.
	out := candidates[:0]
	for _, r := range candidates {
		ok, err := matches(tb, r.Row, where)
		if err != nil {
			return nil, p, err
		}
		if ok {
			out = append(out, r)
		}
	}
	return out, p, nil
}

// checkConditions rejects comparisons that cannot possibly make sense, rather
// than silently returning nothing.
func checkConditions(tb *db.Table, where []Condition) error {
	for _, c := range where {
		i := tb.Def.Schema.ColumnIndex(c.Column)
		if i < 0 {
			return fmt.Errorf("%w: %s.%s", db.ErrNoSuchColumn, tb.Def.Name, c.Column)
		}
		if c.Value.Null {
			continue
		}
		col := tb.Def.Schema.Columns[i]
		if col.Type != c.Value.Type {
			return fmt.Errorf("cannot compare %s column %q with %s", col.Type, col.Name, c.Value)
		}
	}
	return nil
}

func matches(tb *db.Table, row db.Row, where []Condition) (bool, error) {
	for _, c := range where {
		i := tb.Def.Schema.ColumnIndex(c.Column)
		if i < 0 {
			return false, fmt.Errorf("%w: %s", db.ErrNoSuchColumn, c.Column)
		}
		v := row[i]
		if c.Value.Null {
			// "= NULL" is read as IS NULL here, and "!= NULL" as IS NOT NULL.
			isNull := v.Null
			switch c.Op {
			case "=":
				if !isNull {
					return false, nil
				}
			case "!=":
				if isNull {
					return false, nil
				}
			default:
				return false, nil
			}
			continue
		}
		if v.Null {
			return false, nil // NULL compares as unknown, so the row drops out
		}
		cmp := v.Compare(c.Value)
		ok := false
		switch c.Op {
		case "=":
			ok = cmp == 0
		case "!=":
			ok = cmp != 0
		case "<":
			ok = cmp < 0
		case "<=":
			ok = cmp <= 0
		case ">":
			ok = cmp > 0
		case ">=":
			ok = cmp >= 0
		}
		if !ok {
			return false, nil
		}
	}
	return true, nil
}

func (s *Session) query(t *txn.Txn, st *Select) (*Result, error) {
	tb, err := s.d.Table(st.Table)
	if err != nil {
		return nil, err
	}
	rows, p, err := s.rowsFor(t, tb, st.Where)
	if err != nil {
		return nil, err
	}

	// Projection.
	cols := st.Columns
	if cols == nil {
		for _, c := range tb.Def.Schema.Columns {
			cols = append(cols, c.Name)
		}
	}
	idxs := make([]int, len(cols))
	for i, name := range cols {
		j := tb.Def.Schema.ColumnIndex(name)
		if j < 0 {
			return nil, fmt.Errorf("%w: %s.%s", db.ErrNoSuchColumn, st.Table, name)
		}
		idxs[i] = j
	}

	if st.OrderBy != "" {
		j := tb.Def.Schema.ColumnIndex(st.OrderBy)
		if j < 0 {
			return nil, fmt.Errorf("%w: %s.%s", db.ErrNoSuchColumn, st.Table, st.OrderBy)
		}
		// An in-memory sort. A real engine would notice when the chosen index
		// already produces this order and skip it.
		sort.SliceStable(rows, func(a, b int) bool {
			c := rows[a].Row[j].Compare(rows[b].Row[j])
			if st.Desc {
				return c > 0
			}
			return c < 0
		})
	}

	out := make([][]db.Value, 0, len(rows))
	for _, r := range rows {
		if st.Limit >= 0 && len(out) >= st.Limit {
			break
		}
		proj := make([]db.Value, len(idxs))
		for i, j := range idxs {
			proj[i] = r.Row[j]
		}
		out = append(out, proj)
	}
	return &Result{Columns: cols, Rows: out, Plan: p.String()}, nil
}

func (s *Session) insert(t *txn.Txn, st *Insert) (*Result, error) {
	tb, err := s.d.Table(st.Table)
	if err != nil {
		return nil, err
	}
	schema := tb.Def.Schema

	// Map the listed columns onto schema positions; unlisted columns are null.
	positions := make([]int, 0, len(st.Columns))
	for _, name := range st.Columns {
		j := schema.ColumnIndex(name)
		if j < 0 {
			return nil, fmt.Errorf("%w: %s.%s", db.ErrNoSuchColumn, st.Table, name)
		}
		positions = append(positions, j)
	}

	count := 0
	for _, vals := range st.Rows {
		row := make(db.Row, len(schema.Columns))
		for i := range row {
			row[i] = db.NullValue(schema.Columns[i].Type)
		}
		if len(positions) == 0 {
			if len(vals) != len(schema.Columns) {
				return nil, fmt.Errorf("table %s has %d columns, but %d values were given",
					st.Table, len(schema.Columns), len(vals))
			}
			for i, v := range vals {
				cv, err := coerce(schema.Columns[i], v)
				if err != nil {
					return nil, err
				}
				row[i] = cv
			}
		} else {
			if len(vals) != len(positions) {
				return nil, fmt.Errorf("%d columns listed but %d values given", len(positions), len(vals))
			}
			for i, v := range vals {
				cv, err := coerce(schema.Columns[positions[i]], v)
				if err != nil {
					return nil, err
				}
				row[positions[i]] = cv
			}
		}
		if _, err := s.d.Insert(t, tb, row); err != nil {
			return nil, err
		}
		count++
	}
	return &Result{Affected: count, Message: fmt.Sprintf("inserted %d row(s)", count)}, nil
}

func coerce(col db.Column, v db.Value) (db.Value, error) {
	if v.Null {
		return db.NullValue(col.Type), nil
	}
	if v.Type != col.Type {
		return db.Value{}, fmt.Errorf("column %q is %s, but the value %s is %s",
			col.Name, col.Type, v, v.Type)
	}
	return v, nil
}

func (s *Session) update(t *txn.Txn, st *Update) (*Result, error) {
	tb, err := s.d.Table(st.Table)
	if err != nil {
		return nil, err
	}
	rows, _, err := s.rowsFor(t, tb, st.Where)
	if err != nil {
		return nil, err
	}
	// Materialize the target list before touching anything: modifying rows
	// while an index scan is open would let a row be visited twice, once under
	// its old key and once under its new one (the "Halloween problem").
	targets := make([]db.ScanRow, len(rows))
	copy(targets, rows)

	count := 0
	for _, r := range targets {
		row := append(db.Row(nil), r.Row...)
		for _, set := range st.Sets {
			j := tb.Def.Schema.ColumnIndex(set.Column)
			if j < 0 {
				return nil, fmt.Errorf("%w: %s.%s", db.ErrNoSuchColumn, st.Table, set.Column)
			}
			cv, err := coerce(tb.Def.Schema.Columns[j], set.Value)
			if err != nil {
				return nil, err
			}
			row[j] = cv
		}
		if _, err := s.d.UpdateRow(t, tb, r.RID, row); err != nil {
			return nil, err
		}
		count++
	}
	return &Result{Affected: count, Message: fmt.Sprintf("updated %d row(s)", count)}, nil
}

func (s *Session) delete(t *txn.Txn, st *Delete) (*Result, error) {
	tb, err := s.d.Table(st.Table)
	if err != nil {
		return nil, err
	}
	rows, _, err := s.rowsFor(t, tb, st.Where)
	if err != nil {
		return nil, err
	}
	rids := make([]storage.RID, len(rows))
	for i, r := range rows {
		rids[i] = r.RID
	}
	for _, rid := range rids {
		if err := s.d.DeleteRow(t, tb, rid); err != nil {
			return nil, err
		}
	}
	return &Result{Affected: len(rids), Message: fmt.Sprintf("deleted %d row(s)", len(rids))}, nil
}

// --- formatting ------------------------------------------------------------

// String renders a result as a text table.
func (r *Result) String() string {
	if len(r.Columns) == 0 {
		return r.Message
	}
	widths := make([]int, len(r.Columns))
	for i, c := range r.Columns {
		widths[i] = len(c)
	}
	cells := make([][]string, len(r.Rows))
	for i, row := range r.Rows {
		cells[i] = make([]string, len(row))
		for j, v := range row {
			cells[i][j] = v.String()
			if len(cells[i][j]) > widths[j] {
				widths[j] = len(cells[i][j])
			}
		}
	}
	var sb strings.Builder
	writeRow := func(vals []string) {
		for i, v := range vals {
			if i > 0 {
				sb.WriteString(" | ")
			}
			sb.WriteString(v)
			sb.WriteString(strings.Repeat(" ", widths[i]-len(v)))
		}
		sb.WriteString("\n")
	}
	writeRow(r.Columns)
	for i := range r.Columns {
		if i > 0 {
			sb.WriteString("-+-")
		}
		sb.WriteString(strings.Repeat("-", widths[i]))
	}
	sb.WriteString("\n")
	for _, c := range cells {
		writeRow(c)
	}
	fmt.Fprintf(&sb, "(%d row(s))", len(r.Rows))
	return sb.String()
}
