package dba

import (
	"database/sql"
)

type txStmt struct {
	sql     string
	args    []interface{}
	results interface{}
}

type Tx struct {
	tx       *sql.Tx
	db       *sql.DB
	stmtList []*txStmt
}

func (this *Tx) Tx() *sql.Tx {
	return this.tx
}

func (this *Tx) Exec(SQLStr string, args []interface{}, results interface{}) (result sql.Result, err error) {
	//stmt, err := this.tx.Prepare(SQLStr)
	//if err != nil {
	//	this.tx.Rollback()
	//	return nil, err
	//}

	if results != nil {
		var rows *sql.Rows
		rows, err = this.tx.Query(SQLStr, args...)
		if rows != nil {
			err = Scan(rows, results)
		}
	} else {
		result, err = this.tx.Exec(SQLStr, args...)
	}
	if err != nil {
		this.tx.Rollback()
		return nil, err
	}
	return result, nil
}

func (this *Tx) ExecSelectBuilder(sb *SelectBuilder, results interface{}) (err error) {
	sql, args, err := sb.ToSQL()
	if err != nil {
		return err
	}
	_, err = this.Exec(sql, args, results)
	return err
}

func (this *Tx) ExecInsertBuilder(ib *InsertBuilder) (result sql.Result, err error) {
	sql, args, err := ib.ToSQL()
	if err != nil {
		return nil, err
	}
	return this.Exec(sql, args, nil)
}

func (this *Tx) ExecUpdateBuilder(ub *UpdateBuilder) (result sql.Result, err error) {
	sql, args, err := ub.ToSQL()
	if err != nil {
		return nil, err
	}
	return this.Exec(sql, args, nil)
}

func (this *Tx) ExecDeleteBuilder(rb *DeleteBuilder) (result sql.Result, err error) {
	sql, args, err := rb.ToSQL()
	if err != nil {
		return nil, err
	}
	return this.Exec(sql, args, nil)
}

func (this *Tx) ExecBuilder(b *Builder, results interface{}) (result sql.Result, err error) {
	sql, args, err := b.ToSQL()
	if err != nil {
		return nil, err
	}
	return this.Exec(sql, args, results)
}

func (this *Tx) Commit() (err error) {
	//tx, err := this.db.Begin()
	//if err != nil {
	//	tx.Rollback()
	//	return err
	//}
	//
	//for _, ts := range this.stmtList {
	//	stmt, err := tx.Prepare(ts.sql)
	//	if err != nil {
	//		tx.Rollback()
	//		return err
	//	}
	//
	//	var rows *sql.Rows
	//	if ts.results != nil {
	//		rows, err = stmt.Query(ts.args...)
	//	} else {
	//		_, err = stmt.Exec(ts.args...)
	//	}
	//
	//	if err != nil {
	//		tx.Rollback()
	//		return err
	//	}
	//
	//	if rows != nil {
	//		err = Scan(rows, ts.results)
	//		if err != nil {
	//			tx.Rollback()
	//			return err
	//		}
	//	}
	//}

	err = this.tx.Commit()
	if err != nil {
		this.tx.Rollback()
	}
	return err
}

func NewTx(db *sql.DB) (tx *Tx) {
	tx = &Tx{}
	tx.db = db
	var err error
	tx.tx, err = db.Begin()
	if err != nil {
		return nil
	}
	return tx
}
