package dba

import (
	"database/sql"
	"errors"
	"fmt"
	_ "github.com/minus5/gofreetds"
	"github.com/penutty/util"
	"strings"
	"time"
)

const (
	driver = "mssql"

	// Datetime2 is the time.Time format this package uses to communicate DateTime2 values to Moment-Db.
	Datetime2 = "2006-01-02 15:04:05"
)

type Conn struct {
	Db *sql.DB
}

func OpenConn(connstr string) (c *Conn) {
	db, err := sql.Open(driver, connstr)
	if err != nil {
		fmt.Printf("Panicking.\n")
		panic(err)
	}

	c = new(Conn)
	c.Db = db

	return
}

type Trans struct {
	Tx *sql.Tx
}

func OpenTx(connstr string) (t *Trans) {
	var err error
	c := OpenConn(connstr)

	t = new(Trans)
	t.Tx, err = c.Db.Begin()
	if err != nil {
		fmt.Printf("Panicking.\n")
		panic(err)
	}

	return
}

func (t *Trans) Close(err error) {
	if err != nil {
		t.Tx.Rollback()
		return
	}
	t.Tx.Commit()

}

func ParseDateTime2(s string) (t *time.Time, err error) {
	tp, err := time.Parse(Datetime2, s)
	if err != nil {
		return
	}
	t = &tp
	return
}

var ErrorExpectedNotActual = errors.New("db.Exec affected an unpredicted number of rows.")

func ValidateRowsAffected(res sql.Result, expected int) (err error) {
	rows, err := res.RowsAffected()
	if err != nil {
		return
	}

	if rows != int64(expected) {
		err = ErrorExpectedNotActual
		return
	}

	return
}

const (
	Name   = 0
	Alias  = 1
	Schema = 2
	Prefix = 2

	Operator = iota
	Clause
	Args
)

var (
	ErrorEmptySlice            = errors.New("Empty slice passed into function().")
	ErrorEmptyString           = errors.New("Empty string passed into function().")
	ErrorSelectsEmpty          = errors.New("Query.selects is empty.")
	ErrorFromsEmpty            = errors.New("Query.froms is empty.")
	ErrorWheresEmpty           = errors.New("Query.wheres is empty.")
	ErrorArgsParametersEmpty   = errors.New("No arguments passed into Query.Args().")
	ErrorArgsParamsCntNotEqual = errors.New("Argument count does not equal parameter count.")
)

type Column struct {
	prefix string
	name   string
	alias  string
}

func (c Column) String() (s string) {
	s = c.prefix + "." + c.name
	if c.alias != "" {
		s += " AS " + c.alias
	}
	return
}

type Table struct {
	schema string
	name   string
	alias  string
	join   string
}

func (t Table) String() (s string) {
	s = t.schema + "." + t.name + " " + t.alias + "\n"
	if t.join != "" {
		s = "JOIN " + s + "ON " + t.join
	}
	return
}

type Where struct {
	operator string
	clause   string
	args     []interface{}
}

func (w Where) String() string {
	return w.operator + " " + w.clause
}

type Query struct {
	comment string
	columns []*Column
	froms   []*Table
	wheres  []*Where
	err     error
}

func (q *Query) Err() error {
	return q.err
}

func NewQuery(comment string) (q *Query) {
	comment = "--" + comment
	return &Query{
		comment: comment,
	}
}

func (q *Query) SetColumns(columns ...*Column) {
	if q.err != nil {
		return
	}
	if len(columns) <= 0 {
		q.err = ErrorEmptySlice
		return
	}

	for _, col := range columns {
		q.columns = append(q.columns, col)
	}
	return
}

func (q *Query) NewColumn(prefix string, name string, alias string) (c *Column) {
	if q.err != nil {
		return
	}
	if util.IsEmpty(name) || util.IsEmpty(prefix) {
		q.err = ErrorEmptyString
		return
	}
	c = &Column{
		prefix: prefix,
		name:   name,
		alias:  alias,
	}
	return
}

func (q *Query) SetFroms(tables ...*Table) {
	if q.err != nil {
		return
	}
	if len(tables) <= 0 {
		q.err = ErrorEmptySlice
		return
	}

	for _, t := range tables {
		q.froms = append(q.froms, t)
	}
	return
}

func (q *Query) NewTable(schema string, name string, alias string, join string) (t *Table) {
	if q.err != nil {
		return
	}
	if util.IsEmpty(schema) || util.IsEmpty(name) || util.IsEmpty(alias) {
		q.err = ErrorEmptyString
		return
	}
	t = &Table{
		schema: schema,
		name:   name,
		alias:  alias,
		join:   join,
	}
	return
}

func (q *Query) SetWheres(wheres ...*Where) {
	if q.err != nil {
		return
	}
	if util.IsEmpty(wheres) {
		q.err = ErrorEmptySlice
		return
	}

	for _, w := range wheres {
		q.wheres = append(q.wheres, w)
	}
	return
}

func (q *Query) NewWhere(operator string, clause string, args []interface{}) (w *Where) {
	if q.err != nil {
		return
	}
	if util.IsEmpty(clause) {
		q.err = ErrorEmptySlice
		return
	}
	w = &Where{
		operator: operator,
		clause:   clause,
		args:     args,
	}
	return
}

func (q *Query) Build() (queryString string, err error) {
	switch {
	case len(q.columns) <= 0:
		return queryString, ErrorSelectsEmpty
	case len(q.froms) <= 0:
		return queryString, ErrorFromsEmpty
	}

	columnString := make([]string, 0)
	for _, c := range q.columns {
		columnString = append(columnString, c.String())
	}

	fromString := make([]string, 0)
	for _, f := range q.froms {
		fromString = append(fromString, f.String())
	}

	queryString = q.comment + `
				  SELECT ` + strings.Join(columnString, ", ") + `
				  FROM ` + strings.Join(fromString, "\n")

	whereString := make([]string, 0)
	for _, w := range q.wheres {
		whereString = append(whereString, w.String())
	}

	if len(whereString) > 0 {
		queryString = queryString + "\nWHERE " + strings.Join(whereString, "\n")
	}
	return
}

func (q *Query) Args() (as []interface{}, err error) {
	if util.IsEmpty(q.wheres) {
		return as, ErrorWheresEmpty
	}

	for _, w := range q.wheres {
		as = append(as, w.args...)
	}
	return
}
