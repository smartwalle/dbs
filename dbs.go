package dbs

import (
	"context"
	"database/sql"
	"sync"
)

func NewSQL(driver, url string, maxOpen, maxIdle int) (db *sql.DB, err error) {
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

// --------------------------------------------------------------------------------
func NewCache(db DB) DB {
	var c = &StmtCache{}
	c.db = db
	c.stmtCache = make(map[string]*sql.Stmt)
	return c
}

type StmtCache struct {
	db        DB
	mu        sync.Mutex
	stmtCache map[string]*sql.Stmt
}

func (this *StmtCache) Ping() error {
	return this.db.Ping()
}

func (this *StmtCache) PingContext(ctx context.Context) error {
	return this.db.PingContext(ctx)
}

func (this *StmtCache) Prepare(query string) (*sql.Stmt, error) {
	this.mu.Lock()
	defer this.mu.Unlock()

	if stmt, ok := this.stmtCache[query]; ok {
		return stmt, nil
	}
	stmt, err := this.db.Prepare(query)
	if err != nil {
		return nil, err
	}
	this.stmtCache[query] = stmt
	return stmt, nil
}

func (this *StmtCache) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	this.mu.Lock()
	defer this.mu.Unlock()

	if stmt, ok := this.stmtCache[query]; ok {
		return stmt, nil
	}
	stmt, err := this.db.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	this.stmtCache[query] = stmt
	return stmt, nil
}

func (this *StmtCache) Exec(query string, args ...interface{}) (sql.Result, error) {
	stmt, err := this.Prepare(query)
	if err != nil {
		return nil, err
	}
	return stmt.Exec(args...)
}

func (this *StmtCache) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	stmt, err := this.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	return stmt.ExecContext(ctx, args...)
}

func (this *StmtCache) Query(query string, args ...interface{}) (*sql.Rows, error) {
	stmt, err := this.Prepare(query)
	if err != nil {
		return nil, err
	}
	return stmt.Query(args...)
}

func (this *StmtCache) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	stmt, err := this.Prepare(query)
	if err != nil {
		return nil, err
	}
	return stmt.QueryContext(ctx, args...)
}

func (this *StmtCache) Begin() (*sql.Tx, error) {
	return this.db.Begin()
}

func (this *StmtCache) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	return this.db.BeginTx(ctx, opts)
}

// --------------------------------------------------------------------------------
type Executor interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)

	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
}

// --------------------------------------------------------------------------------
type Preparer interface {
	Prepare(query string) (*sql.Stmt, error)
	PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)
}

// --------------------------------------------------------------------------------
type DB interface {
	Executor
	Preparer

	Ping() error
	PingContext(ctx context.Context) error

	Begin() (*sql.Tx, error)
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
}
