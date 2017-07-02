package dbs

import (
	"database/sql"
	"fmt"
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

////////////////////////////////////////////////////////////////////////////////
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

func (this *Session) Query(query string, args ...interface{}) (rows *sql.Rows, err error) {
	//stmt, err := this.Prepare(query)
	//if err != nil {
	//	return nil, err
	//}
	//rows, err = stmt.Query(args...)
	return this.DB.Query(query, args...)
}

func (this *Session) QueryRow(query string, args ...interface{}) (row *sql.Row) {
	//stmt, err := this.Prepare(query)
	//if err != nil {
	//	return nil, err
	//}
	//row = stmt.QueryRow(args...)
	//return row, err
	return this.DB.QueryRow(query, args...)
}

////////////////////////////////////////////////////////////////////////////////
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
