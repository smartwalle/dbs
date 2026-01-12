package dbs

import (
	"context"
	"database/sql"
)

type Tx struct {
	tx *sql.Tx
	db *DB
}

func (tx *Tx) Tx() *sql.Tx {
	return tx.tx
}

func (tx *Tx) Logger() Logger {
	return tx.db.Logger()
}

func (tx *Tx) Mapper() Mapper {
	return tx.db.Mapper()
}

// Prepare 作用同 sql.Tx 的 Prepare 方法。
//
// 本方法返回的 sql.Stmt 不会被缓存，不再使用之后需要调用其 Close 方法将其关闭。
func (tx *Tx) Prepare(query string) (*sql.Stmt, error) {
	return tx.PrepareContext(context.Background(), query)
}

// PrepareContext 作用同 sql.Tx 的 PrepareContext 方法。
//
// 本方法返回的 sql.Stmt 不会被缓存，不再使用之后需要调用其 Close 方法将其关闭。
func (tx *Tx) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	return tx.tx.PrepareContext(ctx, query)
}

func (tx *Tx) Statement(ctx context.Context, query string) (*sql.Stmt, error) {
	var stmt, err = tx.db.statement(ctx, query)
	if err != nil {
		return nil, err
	}
	return tx.tx.StmtContext(ctx, stmt), nil
}

func (tx *Tx) Exec(query string, args ...interface{}) (sql.Result, error) {
	return tx.ExecContext(context.Background(), query, args...)
}

func (tx *Tx) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	stmt, err := tx.Statement(ctx, query)
	if err != nil {
		return nil, err
	}
	result, err := stmt.ExecContext(ctx, args...)
	if err != nil {
		tx.db.removeStatement(query, stmt)
	}
	return result, err
}

func (tx *Tx) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return tx.QueryContext(context.Background(), query, args...)
}

func (tx *Tx) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	stmt, err := tx.Statement(ctx, query)
	if err != nil {
		return nil, err
	}
	rows, err := stmt.QueryContext(ctx, args...)
	if err != nil {
		tx.db.removeStatement(query, stmt)
	}
	return rows, err
}

func (tx *Tx) QueryRow(query string, args ...any) *sql.Row {
	return tx.QueryRowContext(context.Background(), query, args...)
}

func (tx *Tx) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	stmt, err := tx.Statement(ctx, query)
	if err != nil {
		return nil
	}
	row := stmt.QueryRowContext(ctx, args...)
	if row.Err() != nil {
		tx.db.removeStatement(query, stmt)
	}
	return row
}

func (tx *Tx) Commit() error {
	return tx.tx.Commit()
}

func (tx *Tx) Rollback() error {
	return tx.tx.Rollback()
}

func (tx *Tx) ToContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, txSessionKey{}, tx)
}

func TxFromContext(ctx context.Context) *Tx {
	var tx, ok = ctx.Value(txSessionKey{}).(*Tx)
	if ok && tx != nil {
		return tx
	}
	return nil
}
