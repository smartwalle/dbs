package dbs

import (
	"context"
	"database/sql"
)

var ErrNoRows = sql.ErrNoRows
var ErrTxDone = sql.ErrTxDone

type Session interface {
	Dialect() Dialect

	Logger() Logger
	Mapper() Mapper

	Preparer

	Exec(query string, args ...interface{}) (sql.Result, error)
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)

	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)

	QueryRow(query string, args ...any) *sql.Row
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

type Preparer interface {
	Prepare(query string) (*sql.Stmt, error)
	PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)
}

type txSessionKey struct{}

type Database interface {
	Session

	Close() error

	Session(ctx context.Context) Session

	Begin() (*Tx, error)
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*Tx, error)
}

func Open(driver, url string, maxOpen, maxIdle int) (*DB, error) {
	db, err := sql.Open(driver, url)
	if err != nil {
		return nil, err
	}

	if err = db.Ping(); err != nil {
		db.Close()
		return nil, err
	}

	db.SetMaxIdleConns(maxIdle)
	db.SetMaxOpenConns(maxOpen)
	return New(db), err
}

type DB struct {
	db      *sql.DB
	dialect Dialect
	logger  Logger
	mapper  Mapper
}

func New(db *sql.DB) *DB {
	var ndb = &DB{}
	ndb.db = db
	ndb.logger = NewLogger()
	ndb.mapper = NewMapper(kTagSQL)
	return ndb
}

func (db *DB) DB() *sql.DB {
	return db.db
}

func (db *DB) Ping() error {
	return db.db.Ping()
}

func (db *DB) PingContext(ctx context.Context) error {
	return db.db.PingContext(ctx)
}

func (db *DB) Dialect() Dialect {
	return db.dialect
}

func (db *DB) UseDialect(dialect Dialect) {
	if dialect != nil {
		db.dialect = dialect
	}
}

func (db *DB) Logger() Logger {
	return db.logger
}

func (db *DB) UseLogger(logger Logger) {
	if logger != nil {
		db.logger = logger
	}
}

func (db *DB) Mapper() Mapper {
	return db.mapper
}

func (db *DB) UseMapper(mapper Mapper) {
	if mapper != nil {
		db.mapper = mapper
	}
}

func (db *DB) Session(ctx context.Context) Session {
	var session, ok = ctx.Value(txSessionKey{}).(Session)
	if ok && session != nil {
		return session
	}
	return db
}

func (db *DB) Prepare(query string) (*sql.Stmt, error) {
	return db.db.PrepareContext(context.Background(), query)
}

func (db *DB) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	return db.db.PrepareContext(ctx, query)
}

func (db *DB) Exec(query string, args ...interface{}) (sql.Result, error) {
	return db.ExecContext(context.Background(), query, args...)
}

func (db *DB) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return db.db.ExecContext(ctx, query, args...)
}

func (db *DB) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return db.QueryContext(context.Background(), query, args...)
}

func (db *DB) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return db.db.QueryContext(ctx, query, args...)
}

func (db *DB) QueryRow(query string, args ...any) *sql.Row {
	return db.QueryRowContext(context.Background(), query, args...)
}

func (db *DB) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return db.db.QueryRowContext(ctx, query, args...)
}

func (db *DB) Close() error {
	return db.db.Close()
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
	nTx.db = db
	return nTx, nil
}
