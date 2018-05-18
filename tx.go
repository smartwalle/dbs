package dbs

import (
	"context"
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
)

// --------------------------------------------------------------------------------
type TX interface {
	Executor
	Preparer

	Stmt(stmt *sql.Stmt) *sql.Stmt
	StmtContext(ctx context.Context, stmt *sql.Stmt) *sql.Stmt

	Commit() (err error)
	Rollback() error
}

// --------------------------------------------------------------------------------
type StmtCacheTx struct {
	db DB
	tx *sql.Tx
}

func (this *StmtCacheTx) Tx() *sql.Tx {
	return this.tx
}

func (this *StmtCacheTx) Prepare(query string) (*sql.Stmt, error) {
	var stmt, err = this.db.Prepare(query)
	if err != nil {
		return nil, err
	}
	return this.Stmt(stmt), nil
}

func (this *StmtCacheTx) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	var stmt, err = this.db.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	return this.StmtContext(ctx, stmt), nil
}

func (this *StmtCacheTx) Stmt(stmt *sql.Stmt) *sql.Stmt {
	return this.tx.Stmt(stmt)
}

func (this *StmtCacheTx) StmtContext(ctx context.Context, stmt *sql.Stmt) *sql.Stmt {
	return this.tx.StmtContext(ctx, stmt)
}

func (this *StmtCacheTx) Exec(query string, args ...interface{}) (result sql.Result, err error) {
	stmt, err := this.Prepare(query)
	if err != nil {
		return nil, err
	}
	return stmt.Exec(args...)
}

func (this *StmtCacheTx) ExecContext(ctx context.Context, query string, args ...interface{}) (result sql.Result, err error) {
	stmt, err := this.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	return stmt.ExecContext(ctx, args...)
}

func (this *StmtCacheTx) Query(query string, args ...interface{}) (rows *sql.Rows, err error) {
	stmt, err := this.Prepare(query)
	if err != nil {
		return nil, err
	}
	return stmt.Query(args...)
}

func (this *StmtCacheTx) QueryContext(ctx context.Context, query string, args ...interface{}) (rows *sql.Rows, err error) {
	stmt, err := this.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	return stmt.QueryContext(ctx, args...)
}

func (this *StmtCacheTx) Commit() (err error) {
	err = this.tx.Commit()
	return err
}

func (this *StmtCacheTx) Rollback() error {
	return this.tx.Rollback()
}

// --------------------------------------------------------------------------------
func NewTx(db DB) (TX, error) {
	switch db.(type) {
	case *StmtCache:
		var tx = &StmtCacheTx{}
		var err error
		tx.tx, err = db.Begin()
		if err != nil {
			return nil, err
		}
		tx.db = db
		return tx, err
	}
	return db.Begin()
}

func MustTx(db DB) (TX) {
	tx, err := NewTx(db)
	if err != nil {
		panic(err)
	}
	return tx
}

func NewTxContext(ctx context.Context, db DB, opts *sql.TxOptions) (tx TX, err error) {
	switch db.(type) {
	case *StmtCache:
		var tx = &StmtCacheTx{}
		var err error
		tx.tx, err = db.BeginTx(ctx, opts)
		if err != nil {
			return nil, err
		}
		tx.db = db
		return tx, err
	}
	return db.BeginTx(ctx, opts)
}

func MustTxContext(ctx context.Context, db DB, opts *sql.TxOptions) (TX) {
	tx, err := NewTxContext(ctx, db, opts)
	if err != nil {
		panic(err)
	}
	return tx
}