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

func (this *DB) DB() *sql.DB {
	return this.db
}

func (this *DB) Close() error {
	this.cache.Close()
	return this.db.Close()
}

func (this *DB) Ping() error {
	return this.db.Ping()
}

func (this *DB) PingContext(ctx context.Context) error {
	return this.db.PingContext(ctx)
}

func (this *DB) Prepare(query string) (*sql.Stmt, error) {
	return this.PrepareContext(context.Background(), query)
}

func (this *DB) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	return this.db.PrepareContext(ctx, query)
}

func (this *DB) prepare(ctx context.Context, query string) (*sql.Stmt, error) {
	if stmt, found := this.cache.Get(query); found {
		return stmt, nil
	}
	return this.group.Do(query, func(key string) (*sql.Stmt, error) {
		stmt, err := this.db.PrepareContext(ctx, key)
		if err != nil {
			return nil, err
		}
		this.cache.SetEx(key, stmt, time.Now().Add(time.Minute*30).Unix())
		return stmt, err
	})
}

func (this *DB) Exec(query string, args ...interface{}) (sql.Result, error) {
	return this.ExecContext(context.Background(), query, args...)
}

func (this *DB) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	stmt, err := this.prepare(ctx, query)
	if err != nil {
		return nil, err
	}
	return stmt.ExecContext(ctx, args...)
}

func (this *DB) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return this.QueryContext(context.Background(), query, args...)
}

func (this *DB) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	stmt, err := this.prepare(ctx, query)
	if err != nil {
		return nil, err
	}
	return stmt.QueryContext(ctx, args...)
}

func (this *DB) QueryRow(query string, args ...any) *sql.Row {
	return this.QueryRowContext(context.Background(), query, args...)
}

func (this *DB) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	stmt, err := this.prepare(ctx, query)
	if err != nil {
		return nil
	}
	return stmt.QueryRowContext(ctx, args...)
}

func (this *DB) Begin() (*Tx, error) {
	return this.BeginTx(context.Background(), nil)
}

func (this *DB) BeginTx(ctx context.Context, opts *sql.TxOptions) (*Tx, error) {
	tx, err := this.db.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	var nTx = &Tx{}
	nTx.tx = tx
	nTx.preparer = this
	return nTx, err
}

type preparer interface {
	prepare(ctx context.Context, query string) (*sql.Stmt, error)
}

type Tx struct {
	tx       *sql.Tx
	preparer preparer
}

func (this *Tx) TX() *sql.Tx {
	return this.tx
}

func (this *Tx) stmt(ctx context.Context, query string) (*sql.Stmt, error) {
	var stmt, err = this.preparer.prepare(ctx, query)
	if err != nil {
		return nil, err
	}
	return this.tx.StmtContext(ctx, stmt), nil
}

func (this *Tx) Prepare(query string) (*sql.Stmt, error) {
	return this.PrepareContext(context.Background(), query)
}

func (this *Tx) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	return this.tx.PrepareContext(ctx, query)
}

func (this *Tx) Exec(query string, args ...interface{}) (sql.Result, error) {
	return this.ExecContext(context.Background(), query, args...)
}

func (this *Tx) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	if this.preparer != nil {
		stmt, err := this.stmt(ctx, query)
		if err != nil {
			return nil, err
		}
		return stmt.ExecContext(ctx, args...)
	}
	return this.tx.ExecContext(ctx, query, args...)
}

func (this *Tx) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return this.QueryContext(context.Background(), query, args...)
}

func (this *Tx) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	if this.preparer != nil {
		stmt, err := this.stmt(ctx, query)
		if err != nil {
			return nil, err
		}
		return stmt.QueryContext(ctx, args...)
	}
	return this.tx.QueryContext(ctx, query, args...)
}

func (this *Tx) QueryRow(query string, args ...any) *sql.Row {
	return this.QueryRowContext(context.Background(), query, args...)
}

func (this *Tx) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	if this.preparer != nil {
		stmt, err := this.stmt(ctx, query)
		if err != nil {
			return nil
		}
		return stmt.QueryRowContext(ctx, args...)
	}
	return this.tx.QueryRowContext(ctx, query, args...)
}

func (this *Tx) Commit() error {
	return this.tx.Commit()
}

func (this *Tx) Rollback() error {
	return this.tx.Rollback()
}
