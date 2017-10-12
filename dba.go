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

var (
	ErrorEmptyStrings          = errors.New("Empty strings passed into Query.Select().")
	ErrorSelectClauseEmpty     = errors.New("Query.selectClause is empty.")
	ErrorFromClauseEmpty       = errors.New("Query.fromClause is empty.")
	ErrorArgsParametersEmpty   = errors.New("No arguments passed into Query.Args().")
	ErrorArgsParamsCntNotEqual = errors.New("Argument count does not equal parameter count.")
	ErrorColumnDestCntNotEqual = errors.New("Column count does not equal destination count.")
)

type Query struct {
	comment      string
	selectClause []string
	fromClause   string
	joinClause   []string
	whereClause  []string
	args         []interface{}
	destinations []*dest
}

func NewQuery(comment string) (q *Query) {
	comment = "--" + comment
	return &Query{
		comment: comment,
	}
}

func (q *Query) Selects(columns ...string) (err error) {
	if len(columns) <= 0 {
		return ErrorEmptyString
	}

	q.selectClause = append(q.selectClause, columns...)
	return
}

func (q *Query) From(clause string) (err error) {
	if clause == "" {
		return ErrorEmptyString
	}

	q.fromClause = clause
	return
}

func (q *Query) Join(clauses ...string) (err error) {
	if len(clauses) <= 0 {
		return ErrorEmptyString
	}

	q.joinClause = append(q.joinClause, clauses...)
	return
}

func (q *Query) Where(clauses ...string) (err error) {
	if len(clauses) <= 0 {
		return ErrorEmptyString
	}

	q.whereClause = append(q.whereClause, clauses...)
	return
}

func (q *Query) SetArgs(args ...interface{}) (err error) {
	if len(args) <= 0 {
		return ErrorArgsParametersEmpty
	}
	q.args = args
	return
}

func (q *Query) Args() []interface{} {
	return q.args
}

func (q *Query) SetDest(i interface{}, func()
func (q *Query) Dest() []interface{} {
	if util.IsEmpty(q.destinations) {
		return ErrorDestinationsEmpty
	}

	dests := make([]interface{}, len(q.destinations))
	for i, d := range q.destinations {
		dests[i] = d.variable
	}
	return dests
}

type dest struct {
	variable interface{}
	handler  func()
}

func (q *Query) Build() (queryString string, err error) {
	switch {
	case len(q.selectClause) <= 0:
		return ErrorSelectClauseEmpty
	case q.fromClause == "":
		return ErrorFromClauseEmpty
	case q.argsParamsCntNotEqual():
		return ErrorArgsParamsCntNotEqual
	case q.columnDestCntNotEqual():
		return ErrorColumnDestCntNotEqual
	}

	queryString = q.comment + `
				  SELECT ` + strings.Join(q.selectClause, ", ") + `
				  FROM ` + q.fromClause

	if len(q.joinClause) > 0 {
		queryString = queryString + "\nJOIN " + strings.Join(q.joinClause, "\nJOIN ")
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

func (q *Query) columnDestCntNotEqual() bool {
	if len(q.selectClause) != len(q.dest) {
		return true
	}
	return false
}
