package dbs

import (
	"database/sql"
	"context"
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
	p.s = &Session{db}
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

type Session struct {
	*sql.DB
}

// --------------------------------------------------------------------------------
type SQLExecutor interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)

	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
}

// --------------------------------------------------------------------------------
type DB interface {
	SQLExecutor

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
