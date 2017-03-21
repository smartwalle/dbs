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
	db       *sql.DB
	stmtList []*txStmt
}

func (this *Tx) Append(sql string, args []interface{}, results interface{}) *Tx {
	this.stmtList = append(this.stmtList, &txStmt{sql, args, results})
	return this
}

func (this *Tx) AppendSelectBuilder(sb *SelectBuilder, results interface{}) (err error) {
	sql, args, err := sb.ToSQL()
	if err != nil {
		return err
	}
	this.Append(sql, args, results)
	return nil
}

func (this *Tx) AppendInsertBuilder(ib *InsertBuilder) (err error) {
	sql, args, err := ib.ToSQL()
	if err != nil {
		return err
	}
	this.Append(sql, args, nil)
	return nil
}

func (this *Tx) AppendUpdateBuilder(ub *UpdateBuilder) (err error) {
	sql, args, err := ub.ToSQL()
	if err != nil {
		return err
	}
	this.Append(sql, args, nil)
	return nil
}

func (this *Tx) AppendDeleteBuilder(rb *DeleteBuilder) (err error) {
	sql, args, err := rb.ToSQL()
	if err != nil {
		return err
	}
	this.Append(sql, args, nil)
	return nil
}

func (this *Tx) AppendBuilder(b *Builder) (err error) {
	sql, args, err := b.ToSQL()
	if err != nil {
		return err
	}
	this.Append(sql, args, nil)
	return nil
}

func (this *Tx) Commit() (err error) {
	tx, err := this.db.Begin()
	if err != nil {
		tx.Rollback()
		return err
	}

	for _, ts := range this.stmtList {
		stmt, err := tx.Prepare(ts.sql)
		if err != nil {
			tx.Rollback()
			return err
		}

		var rows *sql.Rows
		if ts.results != nil {
			rows, err = stmt.Query(ts.args...)
		} else {
			_, err = stmt.Exec(ts.args...)
		}

		if err != nil {
			tx.Rollback()
			return err
		}

		if rows != nil {
			err = Scan(rows, ts.results)
			if err != nil {
				tx.Rollback()
				return err
			}
		}
	}

	err = tx.Commit()
	if err != nil {
		tx.Rollback()
	}
	return err
}

func NewTx(db *sql.DB) *Tx {
	var tx = &Tx{}
	tx.db = db
	return tx
}
