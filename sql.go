package dbs

import (
	"database/sql"
	"fmt"
	"context"
)

func NewSQL(driver, url string, maxOpen, maxIdle int) (p *Pool) {
	db, err := sql.Open(driver, url)
	if err != nil {
		fmt.Println("连接 SQL 数据库失败:", err)
		return nil
	}

	if err := db.Ping(); err != nil {
		fmt.Println("连接 SQL 数据库失败:", err)
		return nil
	}

	db.SetMaxIdleConns(maxIdle)
	db.SetMaxOpenConns(maxOpen)

	p = &Pool{}
	p.s = &Session{db}
	return p
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

	Begin() (*sql.Tx, error)
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
}

func Exec(s SQLExecutor, query string, args ...interface{}) (sql.Result, error) {
	return s.Exec(query, args...)
}

func ExecContext(ctx context.Context, s SQLExecutor, query string, args ...interface{}) (sql.Result, error) {
	return s.ExecContext(ctx, query, args...)
}

func Query(s SQLExecutor, query string, args ...interface{}) (*sql.Rows, error) {
	return s.Query(query, args...)
}

func QueryContext(ctx context.Context, s SQLExecutor, query string, args ...interface{}) (*sql.Rows, error) {
	return s.QueryContext(ctx, query, args...)
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
