package dbs

import (
	"database/sql"
	"fmt"
	"runtime"
)

type txStmt struct {
	sql     string
	args    []interface{}
	results interface{}
}

type Tx struct {
	id       string
	tx       *sql.Tx
	stmtList []*txStmt
}

func (this *Tx) Tx() *sql.Tx {
	return this.tx
}

func (this *Tx) Query(query string, args ...interface{}) (rows *sql.Rows, err error) {
	defer func() {
		if err != nil {
			this.Rollback()
			rows = nil
		}
	}()
	rows, err = this.tx.Query(query, args...)
	return rows, err
}

func (this *Tx) Exec(query string, args ...interface{}) (result sql.Result, err error) {
	defer func() {
		if err != nil {
			this.Rollback()
			result = nil
		}
	}()
	result, err = this.tx.Exec(query, args...)
	return result, err
}

func (this *Tx) QueryEx(query string, args []interface{}, results interface{}) (err error) {
	_, err = this.exec(query, args, results)
	return err
}

func (this *Tx) exec(query string, args []interface{}, results interface{}) (result sql.Result, err error) {
	defer func() {
		if err != nil {
			this.Rollback()
			result = nil
		}
	}()

	if results != nil {
		var rows *sql.Rows
		rows, err = this.tx.Query(query, args...)
		if rows != nil {
			err = Scan(rows, results)
		}
	} else {
		result, err = this.tx.Exec(query, args...)
	}
	return result, nil
}

func (this *Tx) ExecSelectBuilder(sb *SelectBuilder, results interface{}) (err error) {
	sql, args, err := sb.ToSQL()
	if err != nil {
		this.Rollback()
		return err
	}
	_, err = this.exec(sql, args, results)
	return err
}

func (this *Tx) ExecInsertBuilder(ib *InsertBuilder) (result sql.Result, err error) {
	sql, args, err := ib.ToSQL()
	if err != nil {
		this.Rollback()
		return nil, err
	}
	return this.exec(sql, args, nil)
}

func (this *Tx) ExecUpdateBuilder(ub *UpdateBuilder) (result sql.Result, err error) {
	sql, args, err := ub.ToSQL()
	if err != nil {
		this.Rollback()
		return nil, err
	}
	return this.exec(sql, args, nil)
}

func (this *Tx) ExecDeleteBuilder(rb *DeleteBuilder) (result sql.Result, err error) {
	sql, args, err := rb.ToSQL()
	if err != nil {
		this.Rollback()
		return nil, err
	}
	return this.exec(sql, args, nil)
}

func (this *Tx) ExecBuilder(b *Builder, results interface{}) (result sql.Result, err error) {
	sql, args, err := b.ToSQL()
	if err != nil {
		this.Rollback()
		return nil, err
	}
	return this.exec(sql, args, results)
}

func (this *Tx) Commit() (err error) {
	if debug {
		delete(txStack, this.id)
	}
	err = this.tx.Commit()
	return err
}

func (this *Tx) Rollback() error {
	if debug {
		delete(txStack, this.id)
	}
	return this.tx.Rollback()
}

func NewTx(db *sql.DB) (tx *Tx, err error) {
	tx = &Tx{}
	tx.tx, err = db.Begin()
	if err != nil {
		return nil, err
	}

	if debug {
		tx.id = fmt.Sprintf("%x", &tx)
		pc, file, line, _ := runtime.Caller(1)
		f := runtime.FuncForPC(pc)
		value := fmt.Sprintf("%s-%d-%s", file, line, f.Name())
		txStack[tx.id] = value
	}

	return tx, err
}

var debug = false
var txStack map[string]string

func GetTxStack() map[string]string {
	return txStack
}

func EnableTxDebug() {
	txStack = make(map[string]string)
	debug = true
}

func DisableTxDebug() {
	debug = false
	txStack = nil
}