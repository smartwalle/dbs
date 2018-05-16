package dbs

import (
	"context"
	"database/sql"
	"sync"
)

func NewSQL(driver, url string, maxOpen, maxIdle int) (p *Pool, err error) {
	db, err := sql.Open(driver, url)
	if err != nil {
		return nil, err
	}

	if err := db.Ping(); err != nil {
		return nil, err
	}

	db.SetMaxIdleConns(maxIdle)
	db.SetMaxOpenConns(maxOpen)

	p = &Pool{}
	p.s = NewSession(db)
	return p, nil
}

// --------------------------------------------------------------------------------
type Pool struct {
	s *Session
}

func (this *Pool) GetSession() *Session {
	return this.s
}

func (this *Pool) Release(s *Session) {
	// do nothing
}

// --------------------------------------------------------------------------------
func NewSession(db *sql.DB) *Session {
	var s = &Session{}
	s.DB = db
	s.stmtCache = make(map[string]*sql.Stmt)
	return s
}

type Session struct {
	*sql.DB
	mu        sync.Mutex
	stmtCache map[string]*sql.Stmt
}

func (this *Session) Prepare(query string) (*sql.Stmt, error) {
	this.mu.Lock()
	defer this.mu.Unlock()

	if stmt, ok := this.stmtCache[query]; ok {
		return stmt, nil
	}
	stmt, err := this.DB.Prepare(query)
	if err != nil {
		return nil, err
	}
	this.stmtCache[query] = stmt
	return stmt, nil
}

func (this *Session) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	this.mu.Lock()
	defer this.mu.Unlock()

	if stmt, ok := this.stmtCache[query]; ok {
		return stmt, nil
	}
	stmt, err := this.DB.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	this.stmtCache[query] = stmt
	return stmt, nil
}

func (this *Session) Exec(query string, args ...interface{}) (sql.Result, error) {
	stmt, err := this.Prepare(query)
	if err != nil {
		return nil, err
	}
	return stmt.Exec(args...)
}

func (this *Session) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	stmt, err := this.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	return stmt.ExecContext(ctx, args...)
}

func (this *Session) Query(query string, args ...interface{}) (*sql.Rows, error) {
	stmt, err := this.Prepare(query)
	if err != nil {
		return nil, err
	}
	return stmt.Query(args...)
}

func (this *Session) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	stmt, err := this.Prepare(query)
	if err != nil {
		return nil, err
	}
	return stmt.QueryContext(ctx, args...)
}

func (this *Session) QueryRow(query string, args ...interface{}) *sql.Row {
	stmt, err := this.Prepare(query)
	if err != nil {
		return nil
	}
	return stmt.QueryRow(args...)
}

func (this *Session) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	stmt, err := this.PrepareContext(ctx, query)
	if err != nil {
		return nil
	}
	return stmt.QueryRow(args...)
}

func (this *Session) ExecSQL(query string, args ...interface{}) (sql.Result, error) {
	return this.DB.Exec(query, args...)
}

func (this *Session) QuerySQL(query string, args ...interface{}) (*sql.Rows, error) {
	return this.DB.Query(query, args...)
}

// --------------------------------------------------------------------------------
type SQLExecutor interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)

	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)

	QueryRow(query string, args ...interface{}) *sql.Row
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row

	ExecSQL(query string, args ...interface{}) (sql.Result, error)
	QuerySQL(query string, args ...interface{}) (*sql.Rows, error)
}

// --------------------------------------------------------------------------------
type DB interface {
	SQLExecutor

	Prepare(query string) (*sql.Stmt, error)
	PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)

	Begin() (*sql.Tx, error)
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
}

// --------------------------------------------------------------------------------
const k_SQL_KEY = "sql_session"

type Setter interface {
	Set(key string, value interface{})
}

type Getter interface {
	MustGet(key string) interface{}
}

func FromContext(g Getter) *Session {
	return g.MustGet(k_SQL_KEY).(*Session)
}

func ToContext(s Setter, c *Session) {
	s.Set(k_SQL_KEY, c)
}
