package dbs

import (
	"context"
	"crypto/md5"
	"database/sql"
	"encoding/hex"
	"github.com/smartwalle/dbc"
	"time"
)

func New(driver, url string, maxOpen, maxIdle int) (db *sql.DB, err error) {
	db, err = sql.Open(driver, url)
	if err != nil {
		logger.Println("连接数据库失败:", err)
		return nil, err
	}

	if err := db.Ping(); err != nil {
		return nil, err
	}

	db.SetMaxIdleConns(maxIdle)
	db.SetMaxOpenConns(maxOpen)

	return db, err
}

func NewCache(db DB) *DBCache {
	var c = &DBCache{}
	c.db = db
	c.stmts = dbc.New[*sql.Stmt]()
	c.stmts.OnEvicted(c.onCloseStmt)
	return c
}

type DBCache struct {
	db    DB
	stmts dbc.Cache[string, *sql.Stmt]
}

func (this *DBCache) Close() error {
	this.stmts.Close()
	this.stmts = nil
	return this.db.Close()
}

func (this *DBCache) Ping() error {
	return this.db.Ping()
}

func (this *DBCache) PingContext(ctx context.Context) error {
	return this.db.PingContext(ctx)
}

func (this *DBCache) getStmt(key string) *sql.Stmt {
	var stmt, found = this.stmts.Get(MD5Key(key))
	if found == false {
		return nil
	}
	return stmt
}

func (this *DBCache) putStmt(key string, s *sql.Stmt) {
	this.stmts.SetEx(MD5Key(key), s, time.Now().Add(time.Minute*30).Unix())
}

func (this *DBCache) onCloseStmt(key string, value *sql.Stmt) {
	if value != nil {
		value.Close()
	}
}

func (this *DBCache) Prepare(query string) (*sql.Stmt, error) {
	if stmt := this.getStmt(query); stmt != nil {
		return stmt, nil
	}

	stmt, err := this.db.Prepare(query)
	if err != nil {
		return nil, err
	}
	this.putStmt(query, stmt)
	return stmt, nil
}

func (this *DBCache) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	if stmt := this.getStmt(query); stmt != nil {
		return stmt, nil
	}
	stmt, err := this.db.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	this.putStmt(query, stmt)
	return stmt, nil
}

func (this *DBCache) Exec(query string, args ...interface{}) (sql.Result, error) {
	stmt, err := this.Prepare(query)
	if err != nil {
		return nil, err
	}
	return stmt.Exec(args...)
}

func (this *DBCache) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	stmt, err := this.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	return stmt.ExecContext(ctx, args...)
}

func (this *DBCache) Query(query string, args ...interface{}) (*sql.Rows, error) {
	stmt, err := this.Prepare(query)
	if err != nil {
		return nil, err
	}
	return stmt.Query(args...)
}

func (this *DBCache) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	stmt, err := this.Prepare(query)
	if err != nil {
		return nil, err
	}
	return stmt.QueryContext(ctx, args...)
}

func (this *DBCache) Begin() (*sql.Tx, error) {
	return this.db.Begin()
}

func (this *DBCache) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	return this.db.BeginTx(ctx, opts)
}

func MD5Key(key string) string {
	var a = make([]byte, 0, 16)
	var r = md5.Sum([]byte(key))
	a = append(a, r[:]...)
	return hex.EncodeToString(a)
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
