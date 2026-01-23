package dbs

import (
	"context"
	"database/sql"

	"github.com/smartwalle/dbs/internal"
)

type Tx struct {
	tx *sql.Tx
	db *DB
}

func (tx *Tx) Tx() *sql.Tx {
	return tx.tx
}

func (tx *Tx) Dialect() Dialect {
	return tx.db.Dialect()
}

func (tx *Tx) Logger() Logger {
	return tx.db.Logger()
}

func (tx *Tx) Mapper() Mapper {
	return tx.db.Mapper()
}

func (tx *Tx) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	return tx.tx.PrepareContext(ctx, query)
}

func (tx *Tx) StmtContext(ctx context.Context, stmt *sql.Stmt) *sql.Stmt {
	return tx.tx.StmtContext(ctx, stmt)
}

func (tx *Tx) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return tx.tx.ExecContext(ctx, query, args...)
}

func (tx *Tx) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return tx.tx.QueryContext(ctx, query, args...)
}

func (tx *Tx) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return tx.tx.QueryRowContext(ctx, query, args...)
}

func (tx *Tx) Commit() error {
	return tx.tx.Commit()
}

func (tx *Tx) Rollback() error {
	return tx.tx.Rollback()
}

func (tx *Tx) ToContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, internal.TxSessionKey{}, tx)
}

func TxFromContext(ctx context.Context) *Tx {
	var tx, ok = ctx.Value(internal.TxSessionKey{}).(*Tx)
	if ok && tx != nil {
		return tx
	}
	return nil
}
