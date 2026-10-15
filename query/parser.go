// Package query implements Phase 11: a small SQL front end.
//
// It is deliberately hand written - no parser generator, no SQL library - and
// deliberately small. The point of this layer is not to be SQL, it is to show
// that the storage engine underneath can be driven by one: statements turn
// into calls on transactions, heap files and B+trees, and the planner has to
// decide whether an index is worth using.
//
//	SQL text -> lexer -> tokens -> parser -> AST -> executor -> engine
package query

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"

	"minidb/db"
)

// --- lexer -----------------------------------------------------------------

type tokenKind int

const (
	tokEOF tokenKind = iota
	tokIdent
	tokNumber
	tokString
	tokPunct
)

type token struct {
	kind tokenKind
	text string
	pos  int
}

func (t token) String() string {
	if t.kind == tokEOF {
		return "end of input"
	}
	return strconv.Quote(t.text)
}

type lexer struct {
	src string
	pos int
}

func (l *lexer) next() (token, error) {
	for l.pos < len(l.src) && unicode.IsSpace(rune(l.src[l.pos])) {
		l.pos++
	}
	// Line comments.
	if strings.HasPrefix(l.src[l.pos:], "--") {
		for l.pos < len(l.src) && l.src[l.pos] != '\n' {
			l.pos++
		}
		return l.next()
	}
	if l.pos >= len(l.src) {
		return token{kind: tokEOF, pos: l.pos}, nil
	}
	start := l.pos
	c := l.src[l.pos]

	switch {
	case isIdentStart(c):
		for l.pos < len(l.src) && isIdentPart(l.src[l.pos]) {
			l.pos++
		}
		return token{tokIdent, l.src[start:l.pos], start}, nil

	case c >= '0' && c <= '9', c == '-' && l.pos+1 < len(l.src) && isDigit(l.src[l.pos+1]):
		l.pos++
		for l.pos < len(l.src) && isDigit(l.src[l.pos]) {
			l.pos++
		}
		return token{tokNumber, l.src[start:l.pos], start}, nil

	case c == '\'', c == '"':
		quote := c
		l.pos++
		var sb strings.Builder
		for {
			if l.pos >= len(l.src) {
				return token{}, fmt.Errorf("unterminated string starting at position %d", start)
			}
			ch := l.src[l.pos]
			if ch == quote {
				// '' inside a string is an escaped quote.
				if l.pos+1 < len(l.src) && l.src[l.pos+1] == quote {
					sb.WriteByte(quote)
					l.pos += 2
					continue
				}
				l.pos++
				break
			}
			sb.WriteByte(ch)
			l.pos++
		}
		return token{tokString, sb.String(), start}, nil
	}

	// Two character operators first.
	for _, op := range []string{"<=", ">=", "!=", "<>"} {
		if strings.HasPrefix(l.src[l.pos:], op) {
			l.pos += 2
			return token{tokPunct, op, start}, nil
		}
	}
	l.pos++
	return token{tokPunct, string(c), start}, nil
}

func isDigit(c byte) bool      { return c >= '0' && c <= '9' }
func isIdentStart(c byte) bool { return c == '_' || unicode.IsLetter(rune(c)) }
func isIdentPart(c byte) bool  { return isIdentStart(c) || isDigit(c) }

func tokenize(src string) ([]token, error) {
	l := &lexer{src: src}
	var out []token
	for {
		t, err := l.next()
		if err != nil {
			return nil, err
		}
		out = append(out, t)
		if t.kind == tokEOF {
			return out, nil
		}
	}
}

// --- AST -------------------------------------------------------------------

// Statement is a parsed SQL statement.
type Statement interface{ stmt() }

// CreateTable is CREATE TABLE name (col type, ...).
type CreateTable struct {
	Name    string
	Columns []db.Column
}

// CreateIndex is CREATE [UNIQUE] INDEX name ON table (column).
type CreateIndex struct {
	Name, Table, Column string
	Unique              bool
}

// DropTable is DROP TABLE name.
type DropTable struct{ Name string }

// Insert is INSERT INTO table [(cols)] VALUES (...), (...).
type Insert struct {
	Table   string
	Columns []string
	Rows    [][]db.Value
}

// Condition is one comparison in a WHERE clause.
type Condition struct {
	Column string
	Op     string
	Value  db.Value
}

// Select is SELECT cols FROM table [WHERE ...] [ORDER BY col [DESC]] [LIMIT n].
type Select struct {
	Table   string
	Columns []string // nil means *
	Where   []Condition
	OrderBy string
	Desc    bool
	Limit   int
}

// Assignment is one SET clause of an UPDATE.
type Assignment struct {
	Column string
	Value  db.Value
}

// Update is UPDATE table SET ... [WHERE ...].
type Update struct {
	Table string
	Sets  []Assignment
	Where []Condition
}

// Delete is DELETE FROM table [WHERE ...].
type Delete struct {
	Table string
	Where []Condition
}

// Transaction control.
type Begin struct{}
type Commit struct{}
type Rollback struct{}

// Introspection.
type ShowTables struct{}
type Describe struct{ Table string }
type ShowStats struct{}
type Checkpoint struct{}

// Explain wraps a statement and reports the plan instead of running it.
type Explain struct{ Stmt Statement }

func (*CreateTable) stmt() {}
func (*CreateIndex) stmt() {}
func (*DropTable) stmt()   {}
func (*Insert) stmt()      {}
func (*Select) stmt()      {}
func (*Update) stmt()      {}
func (*Delete) stmt()      {}
func (*Begin) stmt()       {}
func (*Commit) stmt()      {}
func (*Rollback) stmt()    {}
func (*ShowTables) stmt()  {}
func (*Describe) stmt()    {}
func (*ShowStats) stmt()   {}
func (*Checkpoint) stmt()  {}
func (*Explain) stmt()     {}

// --- parser ----------------------------------------------------------------

type parser struct {
	toks []token
	pos  int
}

// Parse turns one SQL statement into an AST.
func Parse(sql string) (Statement, error) {
	toks, err := tokenize(sql)
	if err != nil {
		return nil, err
	}
	p := &parser{toks: toks}
	st, err := p.statement()
	if err != nil {
		return nil, err
	}
	p.accept(";")
	if p.peek().kind != tokEOF {
		return nil, fmt.Errorf("unexpected %s after the end of the statement", p.peek())
	}
	return st, nil
}

func (p *parser) peek() token { return p.toks[p.pos] }

func (p *parser) advance() token {
	t := p.toks[p.pos]
	if t.kind != tokEOF {
		p.pos++
	}
	return t
}

// accept consumes the next token if it matches (case-insensitively).
func (p *parser) accept(text string) bool {
	t := p.peek()
	if t.kind != tokEOF && strings.EqualFold(t.text, text) {
		p.pos++
		return true
	}
	return false
}

func (p *parser) expect(text string) error {
	if p.accept(text) {
		return nil
	}
	return fmt.Errorf("expected %q, found %s", text, p.peek())
}

func (p *parser) ident(what string) (string, error) {
	t := p.peek()
	if t.kind != tokIdent {
		return "", fmt.Errorf("expected %s, found %s", what, t)
	}
	p.pos++
	return t.text, nil
}

func (p *parser) statement() (Statement, error) {
	switch {
	case p.accept("EXPLAIN"):
		inner, err := p.statement()
		if err != nil {
			return nil, err
		}
		return &Explain{Stmt: inner}, nil
	case p.accept("CREATE"):
		return p.createStatement()
	case p.accept("DROP"):
		if err := p.expect("TABLE"); err != nil {
			return nil, err
		}
		name, err := p.ident("a table name")
		return &DropTable{Name: name}, err
	case p.accept("INSERT"):
		return p.insertStatement()
	case p.accept("SELECT"):
		return p.selectStatement()
	case p.accept("UPDATE"):
		return p.updateStatement()
	case p.accept("DELETE"):
		return p.deleteStatement()
	case p.accept("BEGIN"):
		p.accept("TRANSACTION")
		return &Begin{}, nil
	case p.accept("COMMIT"):
		return &Commit{}, nil
	case p.accept("ROLLBACK"), p.accept("ABORT"):
		return &Rollback{}, nil
	case p.accept("CHECKPOINT"):
		return &Checkpoint{}, nil
	case p.accept("SHOW"):
		if p.accept("TABLES") {
			return &ShowTables{}, nil
		}
		if p.accept("STATS") {
			return &ShowStats{}, nil
		}
		return nil, fmt.Errorf("expected TABLES or STATS, found %s", p.peek())
	case p.accept("DESCRIBE"), p.accept("DESC"):
		name, err := p.ident("a table name")
		return &Describe{Table: name}, err
	}
	return nil, fmt.Errorf("unrecognized statement starting at %s", p.peek())
}

func (p *parser) createStatement() (Statement, error) {
	unique := p.accept("UNIQUE")
	if p.accept("INDEX") {
		name, err := p.ident("an index name")
		if err != nil {
			return nil, err
		}
		if err := p.expect("ON"); err != nil {
			return nil, err
		}
		tbl, err := p.ident("a table name")
		if err != nil {
			return nil, err
		}
		if err := p.expect("("); err != nil {
			return nil, err
		}
		col, err := p.ident("a column name")
		if err != nil {
			return nil, err
		}
		if err := p.expect(")"); err != nil {
			return nil, err
		}
		return &CreateIndex{Name: name, Table: tbl, Column: col, Unique: unique}, nil
	}
	if err := p.expect("TABLE"); err != nil {
		return nil, err
	}
	name, err := p.ident("a table name")
	if err != nil {
		return nil, err
	}
	if err := p.expect("("); err != nil {
		return nil, err
	}
	var cols []db.Column
	for {
		cname, err := p.ident("a column name")
		if err != nil {
			return nil, err
		}
		ctype, err := p.ident("a column type")
		if err != nil {
			return nil, err
		}
		t, err := db.ParseType(ctype)
		if err != nil {
			return nil, err
		}
		cols = append(cols, db.Column{Name: cname, Type: t})
		if p.accept(",") {
			continue
		}
		break
	}
	if err := p.expect(")"); err != nil {
		return nil, err
	}
	if len(cols) == 0 {
		return nil, fmt.Errorf("a table needs at least one column")
	}
	return &CreateTable{Name: name, Columns: cols}, nil
}

func (p *parser) insertStatement() (Statement, error) {
	if err := p.expect("INTO"); err != nil {
		return nil, err
	}
	tbl, err := p.ident("a table name")
	if err != nil {
		return nil, err
	}
	st := &Insert{Table: tbl}
	if p.accept("(") {
		for {
			col, err := p.ident("a column name")
			if err != nil {
				return nil, err
			}
			st.Columns = append(st.Columns, col)
			if p.accept(",") {
				continue
			}
			break
		}
		if err := p.expect(")"); err != nil {
			return nil, err
		}
	}
	if err := p.expect("VALUES"); err != nil {
		return nil, err
	}
	for {
		if err := p.expect("("); err != nil {
			return nil, err
		}
		var row []db.Value
		for {
			v, err := p.value()
			if err != nil {
				return nil, err
			}
			row = append(row, v)
			if p.accept(",") {
				continue
			}
			break
		}
		if err := p.expect(")"); err != nil {
			return nil, err
		}
		st.Rows = append(st.Rows, row)
		if p.accept(",") {
			continue
		}
		break
	}
	return st, nil
}

func (p *parser) selectStatement() (Statement, error) {
	st := &Select{Limit: -1}
	if !p.accept("*") {
		for {
			col, err := p.ident("a column name")
			if err != nil {
				return nil, err
			}
			st.Columns = append(st.Columns, col)
			if p.accept(",") {
				continue
			}
			break
		}
	}
	if err := p.expect("FROM"); err != nil {
		return nil, err
	}
	tbl, err := p.ident("a table name")
	if err != nil {
		return nil, err
	}
	st.Table = tbl
	if p.accept("WHERE") {
		st.Where, err = p.whereClause()
		if err != nil {
			return nil, err
		}
	}
	if p.accept("ORDER") {
		if err := p.expect("BY"); err != nil {
			return nil, err
		}
		st.OrderBy, err = p.ident("a column name")
		if err != nil {
			return nil, err
		}
		if p.accept("DESC") {
			st.Desc = true
		} else {
			p.accept("ASC")
		}
	}
	if p.accept("LIMIT") {
		t := p.advance()
		if t.kind != tokNumber {
			return nil, fmt.Errorf("LIMIT needs a number, found %s", t)
		}
		n, err := strconv.Atoi(t.text)
		if err != nil || n < 0 {
			return nil, fmt.Errorf("bad LIMIT %q", t.text)
		}
		st.Limit = n
	}
	return st, nil
}

func (p *parser) updateStatement() (Statement, error) {
	tbl, err := p.ident("a table name")
	if err != nil {
		return nil, err
	}
	if err := p.expect("SET"); err != nil {
		return nil, err
	}
	st := &Update{Table: tbl}
	for {
		col, err := p.ident("a column name")
		if err != nil {
			return nil, err
		}
		if err := p.expect("="); err != nil {
			return nil, err
		}
		v, err := p.value()
		if err != nil {
			return nil, err
		}
		st.Sets = append(st.Sets, Assignment{Column: col, Value: v})
		if p.accept(",") {
			continue
		}
		break
	}
	if p.accept("WHERE") {
		st.Where, err = p.whereClause()
		if err != nil {
			return nil, err
		}
	}
	return st, nil
}
