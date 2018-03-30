package dbs

import (
	"context"
	"database/sql"
)

// --------------------------------------------------------------------------------
type txDB interface {
	Begin() (*sql.Tx, error)
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
}

// --------------------------------------------------------------------------------
type Tx struct {
	db txDB
	tx *sql.Tx
}

func (this *Tx) Tx() *sql.Tx {
	return this.tx
}

func (this *Tx) Begin() (*sql.Tx, error) {
	return this.db.Begin()
}

func (this *Tx) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	return this.db.BeginTx(ctx, opts)
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

func (this *Tx) QueryContext(ctx context.Context, query string, args ...interface{}) (rows *sql.Rows, err error) {
	defer func() {
		if err != nil {
			this.Rollback()
			rows = nil
		}
	}()
	rows, err = this.tx.QueryContext(ctx, query, args...)
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

func (this *Tx) ExecContext(ctx context.Context, query string, args ...interface{}) (result sql.Result, err error) {
	defer func() {
		if err != nil {
			this.Rollback()
			result = nil
		}
	}()
	result, err = this.tx.ExecContext(ctx, query, args...)
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
			defer rows.Close()
			err = Scan(rows, results)
		}
	} else {
		result, err = this.tx.Exec(query, args...)
	}
	return result, err
}

func (this *Tx) execContext(ctx context.Context, query string, args []interface{}, results interface{}) (result sql.Result, err error) {
	defer func() {
		if err != nil {
			this.Rollback()
			result = nil
		}
	}()

	if results != nil {
		var rows *sql.Rows
		rows, err = this.tx.QueryContext(ctx, query, args...)
		if rows != nil {
			defer rows.Close()
			err = Scan(rows, results)
		}
	} else {
		result, err = this.tx.ExecContext(ctx, query, args...)
	}
	return result, err
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

func (this *Tx) ExecSelectBuilderContext(ctx context.Context, sb *SelectBuilder, results interface{}) (err error) {
	sql, args, err := sb.ToSQL()
	if err != nil {
		this.Rollback()
		return err
	}
	_, err = this.execContext(ctx, sql, args, results)
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

func (this *Tx) ExecInsertBuilderContext(ctx context.Context, ib *InsertBuilder) (result sql.Result, err error) {
	sql, args, err := ib.ToSQL()
	if err != nil {
		this.Rollback()
		return nil, err
	}
	return this.execContext(ctx, sql, args, nil)
}

func (this *Tx) ExecUpdateBuilder(ub *UpdateBuilder) (result sql.Result, err error) {
	sql, args, err := ub.ToSQL()
	if err != nil {
		this.Rollback()
		return nil, err
	}
	return this.exec(sql, args, nil)
}

func (this *Tx) ExecUpdateBuilderContext(ctx context.Context, ub *UpdateBuilder) (result sql.Result, err error) {
	sql, args, err := ub.ToSQL()
	if err != nil {
		this.Rollback()
		return nil, err
	}
	return this.execContext(ctx, sql, args, nil)
}

func (this *Tx) ExecDeleteBuilder(rb *DeleteBuilder) (result sql.Result, err error) {
	sql, args, err := rb.ToSQL()
	if err != nil {
		this.Rollback()
		return nil, err
	}
	return this.exec(sql, args, nil)
}

func (this *Tx) ExecDeleteBuilderContext(ctx context.Context, rb *DeleteBuilder) (result sql.Result, err error) {
	sql, args, err := rb.ToSQL()
	if err != nil {
		this.Rollback()
		return nil, err
	}
	return this.execContext(ctx, sql, args, nil)
}

func (this *Tx) ExecBuilder(b Builder, results interface{}) (result sql.Result, err error) {
	sql, args, err := b.ToSQL()
	if err != nil {
		this.Rollback()
		return nil, err
	}
	return this.exec(sql, args, results)
}

func (this *Tx) ExecBuilderContext(ctx context.Context, b Builder, results interface{}) (result sql.Result, err error) {
	sql, args, err := b.ToSQL()
	if err != nil {
		this.Rollback()
		return nil, err
	}
	return this.execContext(ctx, sql, args, results)
}

func (this *Tx) Commit() (err error) {
	err = this.tx.Commit()
	return err
}

func (this *Tx) Rollback() error {
	return this.tx.Rollback()
}

func NewTx(db txDB) (tx *Tx, err error) {
	tx = &Tx{}
	tx.tx, err = db.Begin()
	if err != nil {
		return nil, err
	}
	tx.db = db
	return tx, err
}

func MustTx(db txDB) (tx *Tx) {
	tx, err := NewTx(db)
	if err != nil {
		panic(err)
	}
	return tx
}
