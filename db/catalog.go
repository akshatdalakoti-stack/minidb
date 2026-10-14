package db

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"minidb/storage"
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
