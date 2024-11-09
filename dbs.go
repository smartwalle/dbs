package dbs

import (
	"context"
	"database/sql"
	"errors"
	"github.com/smartwalle/dbc"
	"github.com/smartwalle/nsync/singleflight"
	"time"
)

var ErrNoRows = sql.ErrNoRows
var ErrTxDone = sql.ErrTxDone
var ErrStmtExists = errors.New("statement exists")

type Session interface {
	FromContext(ctx context.Context) Session

	Prepare(query string) (*sql.Stmt, error)
	PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)

	Exec(query string, args ...interface{}) (sql.Result, error)
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)

	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)

	QueryRow(query string, args ...any) *sql.Row
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

type sessionKey struct{}

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

func Open(driver, url string, maxOpen, maxIdle int) (*DB, error) {
	db, err := sql.Open(driver, url)
	if err != nil {
		return nil, err
	}

	if err = db.Ping(); err != nil {
		return nil, err
	}

	db.SetMaxIdleConns(maxIdle)
	db.SetMaxOpenConns(maxOpen)
	return New(db), err
}

func New(db *sql.DB) *DB {
	var ndb = &DB{}
	ndb.db = db
	ndb.cache = dbc.New[*sql.Stmt](dbc.WithHitTTL(60))
	ndb.cache.OnEvicted(func(key string, stmt *sql.Stmt) {
		if stmt != nil {
			stmt.Close()
		}
	})
	ndb.flight = singleflight.NewGroup[string, *sql.Stmt]()
	return ndb
}

type DB struct {
	db     *sql.DB
	cache  dbc.Cache[string, *sql.Stmt]
	flight singleflight.Group[string, *sql.Stmt]
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

func (db *DB) FromContext(ctx context.Context) Session {
	var session, found = ctx.Value(sessionKey{}).(Session)
	if found && session != nil {
		return session
	}
	return db
}

// Prepare 作用同 sql.DB 的 Prepare 方法。
//
// 本方法返回的 sql.Stmt 不会被缓存，不再使用之后需要调用其 Close 方法将其关闭。
func (db *DB) Prepare(query string) (*sql.Stmt, error) {
	return db.db.PrepareContext(context.Background(), query)
}

// PrepareContext 作用同 sql.DB 的 PrepareContext 方法。
//
// 本方法返回的 sql.Stmt 不会被缓存，不再使用之后需要调用其 Close 方法将其关闭。
func (db *DB) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	return db.db.PrepareContext(ctx, query)
}

// PrepareStatement 使用参数 query 创建一个预处理语句(sql.Stmt)并将其缓存，后续可以使用 key 获取该预处理语句。
//
//	var db = dbs.New(...)
//	db.PrepareStatement(ctx, "key", "SELECT ...")
//
//	var stmt, _ = db.Statement(ctx, "key")
//	stmt.QueryContext(ctx, "参数1", "参数2")
//
// 或者
//
//	db.QueryContext(ctx, "key", "参数1", "参数2")
func (db *DB) PrepareStatement(ctx context.Context, key, query string) error {
	if found := db.cache.Exists(key); found {
		return ErrStmtExists
	}
	var _, err = db.flight.Do(key, func(key string) (*sql.Stmt, error) {
		stmt, err := db.db.PrepareContext(ctx, query)
		if err != nil {
			return nil, err
		}
		db.cache.Set(key, stmt)
		return stmt, nil
	})
	return err
}

// RevokeStatement 废弃已缓存的预处理语句(sql.Stmt)。
func (db *DB) RevokeStatement(key string) {
	db.cache.Del(key)
}

// Statement 使用参数 query 获取已经缓存的预处理语句(sql.Stmt)。
//
// 两种情况：
//   - 缓存中若存在，则直接返回；
//   - 缓存中不存在，则根据 query 参数创建一个预处理语句并将其缓存；
//
// 注意：一般不需要直接调用本方法获取预处理语句, 本方法主要是供本结构体的 ExecContext 和 QueryContext 方法使用。
// 如果有从本方法获取预处理语句，不再使用之后不能调用其 Close 方法。
func (db *DB) Statement(ctx context.Context, query string) (*sql.Stmt, error) {
	if stmt, found := db.cache.Get(query); found {
		return stmt, nil
	}
	return db.flight.Do(query, func(key string) (*sql.Stmt, error) {
		stmt, err := db.db.PrepareContext(ctx, key)
		if err != nil {
			return nil, err
		}
		db.cache.SetEx(key, stmt, time.Now().Add(time.Minute*30).Unix())
		return stmt, nil
	})
}

func (db *DB) Exec(query string, args ...interface{}) (sql.Result, error) {
	return db.ExecContext(context.Background(), query, args...)
}

func (db *DB) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	stmt, err := db.Statement(ctx, query)
	if err != nil {
		return nil, err
	}
	return stmt.ExecContext(ctx, args...)
}

func (db *DB) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return db.QueryContext(context.Background(), query, args...)
}

func (db *DB) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	stmt, err := db.Statement(ctx, query)
	if err != nil {
		return nil, err
	}
	return stmt.QueryContext(ctx, args...)
}

func (db *DB) QueryRow(query string, args ...any) *sql.Row {
	return db.QueryRowContext(context.Background(), query, args...)
}

func (db *DB) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	stmt, err := db.Statement(ctx, query)
	if err != nil {
		return nil
	}
	return stmt.QueryRowContext(ctx, args...)
}

func (db *DB) Close() error {
	db.cache.Close()
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
	nTx.preparer = db
	return nTx, nil
}

type Preparer interface {
	Statement(ctx context.Context, query string) (*sql.Stmt, error)
}

type Tx struct {
	tx       *sql.Tx
	preparer Preparer
}

func (tx *Tx) Tx() *sql.Tx {
	return tx.tx
}

func (tx *Tx) FromContext(ctx context.Context) Session {
	return tx
}

// Prepare 作用同 sql.Tx 的 Prepare 方法。
//
// 本方法返回的 sql.Stmt 不会被缓存，不再使用之后需要调用其 Close 方法将其关闭。
func (tx *Tx) Prepare(query string) (*sql.Stmt, error) {
	return tx.PrepareContext(context.Background(), query)
}

// PrepareContext 作用同 sql.Tx 的 PrepareContext 方法。
//
// 本方法返回的 sql.Stmt 不会被缓存，不再使用之后需要调用其 Close 方法将其关闭。
func (tx *Tx) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	return tx.tx.PrepareContext(ctx, query)
}

func (tx *Tx) Statement(ctx context.Context, query string) (*sql.Stmt, error) {
	var stmt, err = tx.preparer.Statement(ctx, query)
	if err != nil {
		return nil, err
	}
	return tx.tx.StmtContext(ctx, stmt), nil
}

func (tx *Tx) Exec(query string, args ...interface{}) (sql.Result, error) {
	return tx.ExecContext(context.Background(), query, args...)
}

func (tx *Tx) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	stmt, err := tx.Statement(ctx, query)
	if err != nil {
		return nil, err
	}
	return stmt.ExecContext(ctx, args...)
}

func (tx *Tx) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return tx.QueryContext(context.Background(), query, args...)
}

func (tx *Tx) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	stmt, err := tx.Statement(ctx, query)
	if err != nil {
		return nil, err
	}
	return stmt.QueryContext(ctx, args...)
}

func (tx *Tx) QueryRow(query string, args ...any) *sql.Row {
	return tx.QueryRowContext(context.Background(), query, args...)
}

func (tx *Tx) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	stmt, err := tx.Statement(ctx, query)
	if err != nil {
		return nil
	}
	return stmt.QueryRowContext(ctx, args...)
}

func (tx *Tx) ToContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, sessionKey{}, tx)
}

func (tx *Tx) Commit() error {
	return tx.tx.Commit()
}

func (tx *Tx) Rollback() error {
	return tx.tx.Rollback()
}
