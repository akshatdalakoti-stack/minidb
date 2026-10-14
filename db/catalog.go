package db

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"

	"minidb/index"
	"minidb/pageops"
	"minidb/storage"
	"minidb/table"
)

// ---------------------------------------------------------------------------
// The catalog: the database's description of itself.
//
// A table's definition has to survive a restart just like its data, so the
// catalog is stored the same way everything else is: as records in a heap
// file, whose meta page id is written into the database file's meta page. On
// open, the engine reads page 0, finds the catalog heap, and reads the tables
// back out of it. There is no separate config file to fall out of sync.
// ---------------------------------------------------------------------------

// Type is a column type.
type Type uint8

const (
	TypeInt Type = iota
	TypeText
)

func (t Type) String() string {
	if t == TypeInt {
		return "INT"
	}
	return "TEXT"
}

// ParseType maps a SQL-ish type name onto a Type.
func ParseType(s string) (Type, error) {
	switch strings.ToUpper(s) {
	case "INT", "INTEGER", "BIGINT":
		return TypeInt, nil
	case "TEXT", "STRING", "VARCHAR":
		return TypeText, nil
	}
	return 0, fmt.Errorf("db: unknown column type %q", s)
}

// Column is one column of a schema.
type Column struct {
	Name string `json:"name"`
	Type Type   `json:"type"`
}

// Schema is an ordered list of columns.
type Schema struct {
	Columns []Column `json:"columns"`
}

// ColumnIndex returns the position of a column, or -1.
func (s Schema) ColumnIndex(name string) int {
	for i, c := range s.Columns {
		if strings.EqualFold(c.Name, name) {
			return i
		}
	}
	return -1
}

func (s Schema) String() string {
	parts := make([]string, len(s.Columns))
	for i, c := range s.Columns {
		parts[i] = c.Name + " " + c.Type.String()
	}
	return strings.Join(parts, ", ")
}

// Value is a single column value.
type Value struct {
	Type Type
	Int  int64
	Text string
	Null bool
}

// IntValue and TextValue construct values.
func IntValue(v int64) Value   { return Value{Type: TypeInt, Int: v} }
func TextValue(s string) Value { return Value{Type: TypeText, Text: s} }

// NullValue is the null of a given type.
func NullValue(t Type) Value { return Value{Type: t, Null: true} }

func (v Value) String() string {
	if v.Null {
		return "NULL"
	}
	if v.Type == TypeInt {
		return strconv.FormatInt(v.Int, 10)
	}
	return v.Text
}

// Compare orders two values of the same type. Nulls sort first.
func (v Value) Compare(o Value) int {
	switch {
	case v.Null && o.Null:
		return 0
	case v.Null:
		return -1
	case o.Null:
		return 1
	}
	if v.Type == TypeInt {
		switch {
		case v.Int < o.Int:
			return -1
		case v.Int > o.Int:
			return 1
		}
		return 0
	}
	return strings.Compare(v.Text, o.Text)
}

// Row is one tuple.
type Row []Value

// Encode serializes a row into a record using the generic field framing from
// the storage layer.
//
// Integers are stored big-endian with the sign bit flipped, which makes the
// byte ordering of the encoding match numeric ordering. That is what lets the
// B+tree - which knows nothing about types and just calls bytes.Compare - do
// range scans over integer columns correctly, including negatives.
func (r Row) Encode(s Schema) (storage.Record, error) {
	if len(r) != len(s.Columns) {
		return nil, fmt.Errorf("db: row has %d values, schema has %d columns", len(r), len(s.Columns))
	}
	fields := make([][]byte, len(r))
	for i, v := range r {
		if v.Null {
			fields[i] = nil // an empty field is null
			continue
		}
		switch s.Columns[i].Type {
		case TypeInt:
			fields[i] = EncodeInt(v.Int)
		default:
			if v.Text == "" {
				// Distinguish the empty string from null with a leading marker.
				fields[i] = []byte{0}
			} else {
				fields[i] = append([]byte{1}, v.Text...)
			}
		}
	}
	return storage.EncodeFields(fields)
}

// DecodeRow is the inverse of Row.Encode.
func DecodeRow(s Schema, rec storage.Record) (Row, error) {
	fields, err := storage.DecodeFields(rec)
	if err != nil {
		return nil, err
	}
	if len(fields) != len(s.Columns) {
		return nil, fmt.Errorf("db: record has %d fields, schema has %d columns", len(fields), len(s.Columns))
	}
	row := make(Row, len(fields))
	for i, f := range fields {
		col := s.Columns[i]
		if len(f) == 0 {
			row[i] = NullValue(col.Type)
			continue
		}
		switch col.Type {
		case TypeInt:
			row[i] = IntValue(DecodeInt(f))
		default:
			row[i] = TextValue(string(f[1:]))
		}
	}
	return row, nil
}

// EncodeInt produces an order-preserving 8 byte key for an int64: big-endian
// with the sign bit flipped, so bytes.Compare agrees with integer comparison.
func EncodeInt(v int64) []byte {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(v)^(1<<63))
	return b[:]
}

// DecodeInt reverses EncodeInt.
func DecodeInt(b []byte) int64 {
	if len(b) < 8 {
		return 0
	}
	return int64(binary.BigEndian.Uint64(b) ^ (1 << 63))
}

// IndexKey encodes a value as an index key.
func IndexKey(v Value) []byte {
	if v.Null {
		return []byte{0} // nulls sort first and share one key
	}
	if v.Type == TypeInt {
		return append([]byte{1}, EncodeInt(v.Int)...)
	}
	return append([]byte{1}, v.Text...)
}

// MinKeyFor / MaxKeyFor bound a range scan over a column type.
func MinKeyFor(t Type) []byte {
	if t == TypeInt {
		return append([]byte{1}, EncodeInt(math.MinInt64)...)
	}
	return []byte{1}
}

// IndexDef describes a secondary index.
type IndexDef struct {
	Name   string         `json:"name"`
	Column string         `json:"column"`
	Meta   storage.PageID `json:"meta"`
	Unique bool           `json:"unique"`
}

// TableDef is a table's persistent definition.
type TableDef struct {
	ID       uint32         `json:"id"`
	Name     string         `json:"name"`
	Schema   Schema         `json:"schema"`
	HeapMeta storage.PageID `json:"heap_meta"`
	Indexes  []IndexDef     `json:"indexes"`
}

// Errors from the catalog.
var (
	ErrTableExists   = errors.New("db: table already exists")
	ErrNoSuchTable   = errors.New("db: no such table")
	ErrNoSuchColumn  = errors.New("db: no such column")
	ErrIndexExists   = errors.New("db: index already exists")
	ErrNoSuchIndex   = errors.New("db: no such index")
	ErrSchemaMismatc = errors.New("db: value does not match the column type")
)

// Catalog holds every table definition, in memory and on disk.
type Catalog struct {
	mu     sync.RWMutex
	heap   *table.Heap
	tables map[string]*Table
	nextID uint32
	rids   map[string]storage.RID // catalog record per table, for updates
}

// Table is a table: its definition, its heap file and its indexes.
type Table struct {
	Def     TableDef
	Heap    *table.Heap
	Indexes []*index.Tree
}

// IndexOn returns the index on a column, if there is one.
func (t *Table) IndexOn(col string) (*index.Tree, IndexDef, bool) {
	for i, d := range t.Def.Indexes {
		if strings.EqualFold(d.Column, col) {
			return t.Indexes[i], d, true
		}
	}
	return nil, IndexDef{}, false
}

// loadCatalog reads the catalog heap into memory, or creates it if this is a
// brand new database.
func loadCatalog(ctx pageops.Ctx, dm *storage.DiskManager) (*Catalog, error) {
	c := &Catalog{tables: map[string]*Table{}, rids: map[string]storage.RID{}, nextID: 1}
	root := dm.CatalogRoot()
	if root == storage.InvalidPageID {
		h, err := table.Create(ctx, "__catalog", 0)
		if err != nil {
			return nil, fmt.Errorf("db: creating catalog: %w", err)
		}
		c.heap = h
		if err := dm.SetCatalogRoot(h.MetaPage()); err != nil {
			return nil, err
		}
		return c, nil
	}
	h, err := table.Open(ctx.Pool, root, "__catalog", 0)
	if err != nil {
		return nil, fmt.Errorf("db: opening catalog: %w", err)
	}
	c.heap = h

	s := h.Scan()
	for s.Next() {
		row := s.Row()
		var def TableDef
		if err := json.Unmarshal(row.Record, &def); err != nil {
			return nil, fmt.Errorf("db: corrupt catalog entry %v: %w", row.RID, err)
		}
		t, err := openTable(ctx, def)
		if err != nil {
			return nil, err
		}
		c.tables[strings.ToLower(def.Name)] = t
		c.rids[strings.ToLower(def.Name)] = row.RID
		if def.ID >= c.nextID {
			c.nextID = def.ID + 1
		}
	}
	return c, s.Err()
}

func openTable(ctx pageops.Ctx, def TableDef) (*Table, error) {
	hp, err := table.Open(ctx.Pool, def.HeapMeta, def.Name, def.ID)
	if err != nil {
		return nil, err
	}
	t := &Table{Def: def, Heap: hp}
	for _, id := range def.Indexes {
		t.Indexes = append(t.Indexes, index.Open(ctx.Pool, id.Meta, id.Name))
	}
	return t, nil
}

// Get returns a table by name.
func (c *Catalog) Get(name string) (*Table, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	t, ok := c.tables[strings.ToLower(name)]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrNoSuchTable, name)
	}
	return t, nil
}

// Tables lists the table names in the database.
func (c *Catalog) Tables() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]string, 0, len(c.tables))
	for _, t := range c.tables {
		out = append(out, t.Def.Name)
	}
	return out
}

// createTable adds a table: a heap file plus a catalog record.
func (c *Catalog) createTable(ctx pageops.Ctx, name string, schema Schema) (*Table, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	key := strings.ToLower(name)
	if _, exists := c.tables[key]; exists {
		return nil, fmt.Errorf("%w: %q", ErrTableExists, name)
	}
	id := c.nextID
	hp, err := table.Create(ctx, name, id)
	if err != nil {
		return nil, err
	}
	def := TableDef{ID: id, Name: name, Schema: schema, HeapMeta: hp.MetaPage()}
	rid, err := c.writeDef(ctx, def)
	if err != nil {
		return nil, err
	}
	c.nextID++
	t := &Table{Def: def, Heap: hp}
	c.tables[key] = t
	c.rids[key] = rid
	return t, nil
}
