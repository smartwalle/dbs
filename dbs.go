package dbs

import (
	"context"
	"database/sql"
	"github.com/smartwalle/dbc"
	"github.com/smartwalle/nsync/singleflight"
	"time"
)

var ErrNoRows = sql.ErrNoRows
var ErrTxDone = sql.ErrTxDone

type Session interface {
	Prepare(query string) (*sql.Stmt, error)
	PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)

	Exec(query string, args ...interface{}) (sql.Result, error)
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)

	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)

	QueryRow(query string, args ...any) *sql.Row
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

type Database interface {
	Session

	Close() error

	Begin() (*Tx, error)
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*Tx, error)
}

type Transaction interface {
	Session

	Rollback() error
	Commit() error
}

func Open(driver, url string, maxOpen, maxIdle int) (db *sql.DB, err error) {
	db, err = sql.Open(driver, url)
	if err != nil {
		return nil, err
	}

	if err = db.Ping(); err != nil {
		return nil, err
	}

	db.SetMaxIdleConns(maxIdle)
	db.SetMaxOpenConns(maxOpen)
	return db, err
}

func New(db *sql.DB) *DB {
	var ndb = &DB{}
	ndb.db = db
	ndb.cache = dbc.New[*sql.Stmt](dbc.WithHitTTL(60))
	ndb.cache.OnEvicted(func(key string, value *sql.Stmt) {
		if value != nil {
			value.Close()
		}
	})
	ndb.group = singleflight.NewGroup[string, *sql.Stmt]()
	return ndb
}

type DB struct {
	db    *sql.DB
	cache dbc.Cache[string, *sql.Stmt]
	group singleflight.Group[string, *sql.Stmt]
}

func (db *DB) DB() *sql.DB {
	return db.db
}

func (db *DB) Close() error {
	db.cache.Close()
	return db.db.Close()
}

func (db *DB) Ping() error {
	return db.db.Ping()
}

func (db *DB) PingContext(ctx context.Context) error {
	return db.db.PingContext(ctx)
}

func (db *DB) Prepare(query string) (*sql.Stmt, error) {
	return db.PrepareContext(context.Background(), query)
}

func (db *DB) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	return db.db.PrepareContext(ctx, query)
}

func (db *DB) PreparedStatement(ctx context.Context, query string) (*sql.Stmt, error) {
	if stmt, found := db.cache.Get(query); found {
		return stmt, nil
	}
	return db.group.Do(query, func(key string) (*sql.Stmt, error) {
		stmt, err := db.db.PrepareContext(ctx, key)
		if err != nil {
			return nil, err
		}
		db.cache.SetEx(key, stmt, time.Now().Add(time.Minute*30).Unix())
		return stmt, nil
	})
}

func (db *DB) Exec(query string, args ...interface{}) (sql.Result, error) {
	return db.ExecContext(context.Background(), query, args...)
}

func (db *DB) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	stmt, err := db.PreparedStatement(ctx, query)
	if err != nil {
		return nil, err
	}
	return stmt.ExecContext(ctx, args...)
}

func (db *DB) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return db.QueryContext(context.Background(), query, args...)
}

func (db *DB) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	stmt, err := db.PreparedStatement(ctx, query)
	if err != nil {
		return nil, err
	}
	return stmt.QueryContext(ctx, args...)
}

func (db *DB) QueryRow(query string, args ...any) *sql.Row {
	return db.QueryRowContext(context.Background(), query, args...)
}

func (db *DB) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	stmt, err := db.PreparedStatement(ctx, query)
	if err != nil {
		return nil
	}
	return stmt.QueryRowContext(ctx, args...)
}

func (db *DB) Begin() (*Tx, error) {
	return db.BeginTx(context.Background(), nil)
}

func (db *DB) BeginTx(ctx context.Context, opts *sql.TxOptions) (*Tx, error) {
	tx, err := db.db.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	var nTx = &Tx{}
	nTx.tx = tx
	nTx.preparer = db
	return nTx, nil
}

type Preparer interface {
	PreparedStatement(ctx context.Context, query string) (*sql.Stmt, error)
}

type Tx struct {
	tx       *sql.Tx
	preparer Preparer
}

func (tx *Tx) Tx() *sql.Tx {
	return tx.tx
}

func (tx *Tx) stmt(ctx context.Context, query string) (*sql.Stmt, error) {
	var stmt, err = tx.preparer.PreparedStatement(ctx, query)
	if err != nil {
		return nil, err
	}
	return tx.tx.StmtContext(ctx, stmt), nil
}

func (tx *Tx) Prepare(query string) (*sql.Stmt, error) {
	return tx.PrepareContext(context.Background(), query)
}

func (tx *Tx) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	return tx.tx.PrepareContext(ctx, query)
}

func (tx *Tx) Exec(query string, args ...interface{}) (sql.Result, error) {
	return tx.ExecContext(context.Background(), query, args...)
}

func (tx *Tx) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	if tx.preparer != nil {
		stmt, err := tx.stmt(ctx, query)
		if err != nil {
			return nil, err
		}
		return stmt.ExecContext(ctx, args...)
	}
	return tx.tx.ExecContext(ctx, query, args...)
}

func (tx *Tx) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return tx.QueryContext(context.Background(), query, args...)
}

func (tx *Tx) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	if tx.preparer != nil {
		stmt, err := tx.stmt(ctx, query)
		if err != nil {
			return nil, err
		}
		return stmt.QueryContext(ctx, args...)
	}
	return tx.tx.QueryContext(ctx, query, args...)
}

func (tx *Tx) QueryRow(query string, args ...any) *sql.Row {
	return tx.QueryRowContext(context.Background(), query, args...)
}

func (tx *Tx) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	if tx.preparer != nil {
		stmt, err := tx.stmt(ctx, query)
		if err != nil {
			return nil
		}
		return stmt.QueryRowContext(ctx, args...)
	}
	return tx.tx.QueryRowContext(ctx, query, args...)
}

func (tx *Tx) Commit() error {
	return tx.tx.Commit()
}

func (tx *Tx) Rollback() error {
	return tx.tx.Rollback()
}
