package dbs

import (
	"context"
	"database/sql"
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

//
//func NewCache(db DB) *DBCache {
//	var c = &DBCache{}
//	c.db = db
//	c.stmts = dbc.New()
//	return c
//}
//
//type DBCache struct {
//	db    DB
//	stmts dbc.Cache
//}
//
//func (this *DBCache) Close() error {
//	this.stmts.Close()
//	this.stmts = nil
//	return this.db.Close()
//}
//
//func (this *DBCache) Ping() error {
//	return this.db.Ping()
//}
//
//func (this *DBCache) PingContext(ctx context.Context) error {
//	return this.db.PingContext(ctx)
//}
//
//func (this *DBCache) getStmt(key string) *sql.Stmt {
//	var v = this.stmts.Get(md5Key(key))
//	if v == nil {
//		return nil
//	}
//	stmt, ok := v.(*sql.Stmt)
//	if ok == false {
//		return nil
//	}
//	return stmt
//}
//
//func (this *DBCache) putStmt(key string, s *sql.Stmt) {
//	this.stmts.Set(md5Key(key), s, time.Minute*30)
//}
//
//func (this *DBCache) onCloseStmt(key string, value interface{}) {
//	if stmt, ok := value.(*sql.Stmt); ok {
//		stmt.Close()
//	}
//}
//
//func (this *DBCache) Prepare(query string) (*sql.Stmt, error) {
//	if stmt := this.getStmt(query); stmt != nil {
//		return stmt, nil
//	}
//
//	stmt, err := this.db.Prepare(query)
//	if err != nil {
//		return nil, err
//	}
//	this.putStmt(query, stmt)
//	return stmt, nil
//}
//
//func (this *DBCache) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
//	if stmt := this.getStmt(query); stmt != nil {
//		return stmt, nil
//	}
//	stmt, err := this.db.PrepareContext(ctx, query)
//	if err != nil {
//		return nil, err
//	}
//	this.putStmt(query, stmt)
//	return stmt, nil
//}
//
//func (this *DBCache) Exec(query string, args ...interface{}) (sql.Result, error) {
//	stmt, err := this.Prepare(query)
//	if err != nil {
//		return nil, err
//	}
//	return stmt.Exec(args...)
//}
//
//func (this *DBCache) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
//	stmt, err := this.PrepareContext(ctx, query)
//	if err != nil {
//		return nil, err
//	}
//	return stmt.ExecContext(ctx, args...)
//}
//
//func (this *DBCache) Query(query string, args ...interface{}) (*sql.Rows, error) {
//	stmt, err := this.Prepare(query)
//	if err != nil {
//		return nil, err
//	}
//	return stmt.Query(args...)
//}
//
//func (this *DBCache) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
//	stmt, err := this.Prepare(query)
//	if err != nil {
//		return nil, err
//	}
//	return stmt.QueryContext(ctx, args...)
//}
//
//func (this *DBCache) Begin() (*sql.Tx, error) {
//	return this.db.Begin()
//}
//
//func (this *DBCache) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
//	return this.db.BeginTx(ctx, opts)
//}
//
//func md5Key(key string) string {
//	var a = make([]byte, 0, 16)
//	var r = md5.Sum([]byte(key))
//	a = append(a, r[:]...)
//	return hex.EncodeToString(a)
//}

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
