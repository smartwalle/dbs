package dbs

import (
	"context"
	"database/sql"
	"github.com/smartwalle/dbc"
	"github.com/smartwalle/nsync/singleflight"
	"time"
)

var ErrNoRows = sql.ErrNoRows

func New(driver, url string, maxOpen, maxIdle int) (db *sql.DB, err error) {
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

func Wrap(db *sql.DB) DB {
	var ndb = &dbsDB{}
	ndb.db = db
	ndb.cache = dbc.New[*sql.Stmt]()
	ndb.cache.OnEvicted(func(key string, value *sql.Stmt) {
		if value != nil {
			value.Close()
		}
	})
	ndb.group = singleflight.NewGroup[string, *sql.Stmt]()
	return ndb
}

type dbsDB struct {
	db    *sql.DB
	cache dbc.Cache[string, *sql.Stmt]
	group singleflight.Group[string, *sql.Stmt]
}

func (this *dbsDB) Close() error {
	this.cache.Close()
	return this.db.Close()
}

func (this *dbsDB) Ping() error {
	return this.db.Ping()
}

func (this *dbsDB) PingContext(ctx context.Context) error {
	return this.db.PingContext(ctx)
}

func (this *dbsDB) Prepare(query string) (*sql.Stmt, error) {
	return this.PrepareContext(context.Background(), query)
}

func (this *dbsDB) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
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

func (this *dbsDB) Exec(query string, args ...interface{}) (sql.Result, error) {
	stmt, err := this.Prepare(query)
	if err != nil {
		return nil, err
	}
	return stmt.Exec(args...)
}

func (this *dbsDB) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	stmt, err := this.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	return stmt.ExecContext(ctx, args...)
}

func (this *dbsDB) Query(query string, args ...interface{}) (*sql.Rows, error) {
	stmt, err := this.Prepare(query)
	if err != nil {
		return nil, err
	}
	return stmt.Query(args...)
}

func (this *dbsDB) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	stmt, err := this.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	return stmt.QueryContext(ctx, args...)
}

func (this *dbsDB) Begin() (*sql.Tx, error) {
	return this.db.Begin()
}

func (this *dbsDB) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	return this.db.BeginTx(ctx, opts)
}

type Session interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)

	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)

	Prepare(query string) (*sql.Stmt, error)
	PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)
}

type DB interface {
	Session

	Close() error

	Ping() error
	PingContext(ctx context.Context) error

	Begin() (*sql.Tx, error)
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
}
