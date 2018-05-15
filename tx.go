package dbs

import (
	"context"
	"database/sql"
)

// --------------------------------------------------------------------------------
type Tx struct {
	db DB
	tx *sql.Tx
}

func (this *Tx) Tx() *sql.Tx {
	return this.tx
}

func (this *Tx) Prepare(query string) (*sql.Stmt, error) {
	var stmt, err = this.db.Prepare(query)
	if err != nil {
		return nil, err
	}
	return this.tx.Stmt(stmt), nil
}

func (this *Tx) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	var stmt, err = this.db.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	return this.tx.StmtContext(ctx, stmt), nil
}

func (this *Tx) Exec(query string, args ...interface{}) (result sql.Result, err error) {
	defer func() {
		if err != nil {
			this.Rollback()
			result = nil
		}
	}()
	stmt, err := this.Prepare(query)
	if err != nil {
		return nil, err
	}
	return stmt.Exec(args...)
}

func (this *Tx) ExecContext(ctx context.Context, query string, args ...interface{}) (result sql.Result, err error) {
	defer func() {
		if err != nil {
			this.Rollback()
			result = nil
		}
	}()
	stmt, err := this.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	return stmt.ExecContext(ctx, args...)
}

func (this *Tx) Query(query string, args ...interface{}) (rows *sql.Rows, err error) {
	defer func() {
		if err != nil {
			this.Rollback()
			rows = nil
		}
	}()
	stmt, err := this.Prepare(query)
	if err != nil {
		return nil, err
	}
	return stmt.Query(args...)
}

func (this *Tx) QueryContext(ctx context.Context, query string, args ...interface{}) (rows *sql.Rows, err error) {
	defer func() {
		if err != nil {
			this.Rollback()
			rows = nil
		}
	}()
	stmt, err := this.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	return stmt.QueryContext(ctx, args...)
}

func (this *Tx) QueryRow(query string, args ...interface{}) *sql.Row {
	stmt, err := this.Prepare(query)
	if err != nil {
		this.Rollback()
		return nil
	}
	return stmt.QueryRow(args...)
}

func (this *Tx) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	stmt, err := this.PrepareContext(ctx, query)
	if err != nil {
		this.Rollback()
		return nil
	}
	return stmt.QueryRow(args...)
}

func (this *Tx) QueryEx(query string, args []interface{}, results interface{}) (err error) {
	_, err = this.exec(query, args, results)
	return err
}

func (this *Tx) ExecRaw(query string, args ...interface{}) (result sql.Result, err error) {
	defer func() {
		if err != nil {
			this.Rollback()
			result = nil
		}
	}()
	return this.tx.Exec(query, args...)
}

func (this *Tx) QueryRaw(query string, args ...interface{}) (rows *sql.Rows, err error) {
	defer func() {
		if err != nil {
			this.Rollback()
			rows = nil
		}
	}()
	return this.tx.Query(query, args...)
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
		rows, err = this.Query(query, args...)
		if rows != nil {
			defer rows.Close()
			err = Scan(rows, results)
		}
	} else {
		result, err = this.Exec(query, args...)
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

func NewTx(db DB) (tx *Tx, err error) {
	tx = &Tx{}
	tx.tx, err = db.Begin()
	if err != nil {
		return nil, err
	}
	tx.db = db
	return tx, err
}

func NewTxContext(ctx context.Context, db DB, opts *sql.TxOptions) (tx *Tx, err error) {
	tx = &Tx{}
	tx.tx, err = db.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	tx.db = db
	return tx, err
}

func MustTx(db DB) (tx *Tx) {
	tx, err := NewTx(db)
	if err != nil {
		panic(err)
	}
	return tx
}

func MustTxContext(ctx context.Context, db DB, opts *sql.TxOptions) (tx *Tx) {
	tx, err := NewTxContext(ctx, db, opts)
	if err != nil {
		panic(err)
	}
	return tx
}
