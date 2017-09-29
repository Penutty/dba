package dba

import (
	"database/sql"
	"errors"
	"fmt"
	_ "github.com/minus5/gofreetds"
)

var driver = "mssql"
var momentConnStr = "Server=192.168.1.4:1433;Database=Moment-Db;User Id=Reader;Password=123"

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

func (c *Conn) Close() {
	c.Db.Close()
	if r := recover(); r != nil {
		fmt.Printf("Recovering.\n")
	}
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

func ValidateTx(t *Trans) (f func()) {
	if t == nil {
		f = func() {
			c := OpenTx()
			defer c.Close()
		}
	}
	return f
}

func (t *Trans) Close() func(err error) {
	return func(err error) {
		if r := recover(); r != nil {
			fmt.Printf("Recovering.\n")
			return
		}

		if err != nil {
			t.Tx.Rollback()
			return
		}
		t.Tx.Commit()
	}
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
