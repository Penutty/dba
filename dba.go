package dba

import (
	"database/sql"
	"errors"
	"fmt"
	_ "github.com/minus5/gofreetds"
	"strings"
)

var driver = "mssql"
var momentConnStr = "Server=192.168.1.2:1433;Database=Moment-Db;User Id=Reader;Password=123"

type Conn struct {
	Db *sql.DB
}

func OpenConn() (c *Conn) {
	db, err := sql.Open(driver, momentConnStr)
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

func OpenTx() (t *Trans) {
	var err error
	c := OpenConn()

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
	ErrorEmptyString           = errors.New("Empty string passed into function().")
	ErrorSelectClauseEmpty     = errors.New("Query.selectClause is empty.")
	ErrorFromClauseEmpty       = errors.New("Query.fromClause is empty.")
	ErrorArgsParametersEmpty   = errors.New("No arguments passed into Query.Args().")
	ErrorArgsParamsCntNotEqual = errors.New("Argument count does not equal parameter count.")
	ErrorColumnDestCntNotEqual = errors.New("Column count does not equal destination count.")
)

type column struct {
	prefix string
	name   string
	alias  string
	dest   interface{}
}

type table struct {
	schema string
	name   string
	alias  string
}

type where struct {
	logicalOp string
	clause    string
	args      []interface{}
}

type Query struct {
	comment string
	columns []*column
	froms   []*table
	wheres  []*where
}

func NewQuery(comment string) (q *Query) {
	comment = "--" + comment
	return &Query{
		comment: comment,
	}
}

func (q *Query) SetColumns(s ...[]string) (err error) {
	if len(s) <= 0 {
		return ErrorEmptySlice
	}

	for _, c := range s {
		col, err := newColumn(c[Prefix], c[Name], c[Alias])
		if err != nil {
			return
		}
		q.columns = append(q.columns, col)
	}
	return
}

func newColumn(name string, alias string) (c *column, err error) {
	if util.IsEmpty(name) || util.IsEmpty(prefix) {
		return ErrorEmptyString
	}
	var i interface{}
	c = &column{
		prefix: prefix,
		name:   name,
		alias:  alias,
		dest:   i,
	}
	return
}

func (q *Query) SetFroms(s ...[]string) (err error) {
	if len(s) <= 0 {
		return ErrorEmptySlice
	}

	for _, f := range s {
		t, err := newTable(f[Schema], f[Name], f[Alias])
		q.froms = append(q.froms, t)
	}
	return
}

func newTable(schema string, name string, alias string) (t *table, err error) {
	if util.IsEmpty(schema, name, alias) {
		return
	}
	t = &table{
		schema: schema,
		name:   name,
		alias:  alias,
	}
	return
}

func (q *Query) SetWheres(s ...[]string) (err error) {
	if util.IsEmpty(s[Clause]) {
		return ErrorEmptySlice
	}

	for _, v := range s {
		w, err := newWhere(v[Operator], v[Clause], v[Args])
		if err != nil {
			return
		}
		q.whereClause = append(q.whereClause, w)
	}
	return
}

func newWhere(operator string, clause string, args []interface{}) (w *where, err error) {
	if util.IsEmpty(clause) {
		return
	}
	w = &where{
		logicalOp: logicalOp,
		clause:    clause,
		args:      args,
	}
	return
}

func (q *Query) Build() (queryString string, err error) {
	switch {
	case len(q.selectClause) <= 0:
		return queryString, ErrorSelectClauseEmpty
	case len(q.fromClause) <= 0:
		return queryString, ErrorFromClauseEmpty
	case q.argsParamsCntNotEqual():
		return queryString, ErrorArgsParamsCntNotEqual
	}

	queryString = q.comment + `
				  SELECT ` + strings.Join(q.selectClause, ", ") + "\n"

	for i, c := range q.fromClause {
		if i == 0 {
			queryString += `FROM ` + c + "\n"
		} else {
			queryString += `JOIN ` + c + "\n"
		}
	}

	if len(q.whereClause) > 0 {
		queryString = queryString + "\nWHERE " + strings.Join(q.whereClause, "\n")
	}
	return
}

func (q *Query) argsParamsCntNotEqual() bool {
	if strings.Count(strings.Join(q.whereClause, " "), "?") != len(q.args) {
		return true
	}
	return false
}
