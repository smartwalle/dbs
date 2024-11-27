package dbs

import (
	"context"
	"database/sql"
	"sync"
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

type sessionKey struct{}

type Database interface {
	Session

	Close() error

	Session(ctx context.Context) Session

	Begin() (*Tx, error)
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*Tx, error)
}

type Transaction interface {
	Session

	Rollback() error
	Commit() error
}

type Option func(db *DB)

func WithTimeout(timeout time.Duration) Option {
	return func(db *DB) {
		db.prepareTimeout = timeout
		db.execTimeout = timeout
		db.queryTimeout = timeout
	}
}

func WithTxTimeout(timeout time.Duration) Option {
	return func(db *DB) {
		db.txTimeout = timeout
	}
}

func WithPrepareTimeout(timeout time.Duration) Option {
	return func(db *DB) {
		db.prepareTimeout = timeout
	}
}

func WithExecTimeout(timeout time.Duration) Option {
	return func(db *DB) {
		db.execTimeout = timeout
	}
}

func WithQueryTimeout(timeout time.Duration) Option {
	return func(db *DB) {
		db.queryTimeout = timeout
	}
}

func Open(driver, url string, maxOpen, maxIdle int, opts ...Option) (*DB, error) {
	db, err := sql.Open(driver, url)
	if err != nil {
		return nil, err
	}

	if err = db.Ping(); err != nil {
		return nil, err
	}

	db.SetMaxIdleConns(maxIdle)
	db.SetMaxOpenConns(maxOpen)
	return New(db, opts...), err
}

func New(db *sql.DB, opts ...Option) *DB {
	var ndb = &DB{}
	ndb.db = db
	ndb.mu = &sync.RWMutex{}
	ndb.txTimeout = 0
	ndb.prepareTimeout = time.Second * 3
	ndb.execTimeout = time.Second * 3
	ndb.queryTimeout = time.Second * 3
	ndb.stmts = make(map[string]*Stmt)
	for _, opt := range opts {
		if opt != nil {
			opt(ndb)
		}
	}
	return ndb
}

type DB struct {
	db             *sql.DB
	mu             *sync.RWMutex
	txTimeout      time.Duration
	prepareTimeout time.Duration
	execTimeout    time.Duration
	queryTimeout   time.Duration
	stmts          map[string]*Stmt
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

func (db *DB) Session(ctx context.Context) Session {
	var session, exists = ctx.Value(sessionKey{}).(Session)
	if exists && session != nil {
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
	if _, ok := ctx.Deadline(); !ok && db.prepareTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, db.prepareTimeout)
		defer cancel()
	}
	return db.db.PrepareContext(ctx, query)
}

// PrepareStatement 使用参数 query 创建一个预处理语句(sql.Stmt)并将其缓存，后续可以使用 key 使用该预处理语句。
//
//	var db = dbs.New(...)
//	db.PrepareStatement(ctx, "key", "SELECT ...")
//
//	db.QueryContext(ctx, "key", "参数1", "参数2")
func (db *DB) PrepareStatement(ctx context.Context, key, query string) error {
	if _, ok := ctx.Deadline(); !ok && db.prepareTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, db.prepareTimeout)
		defer cancel()
	}
	_, err := db.prepareStatement(ctx, key, query)
	return err
}

func (db *DB) prepareStatement(ctx context.Context, key, query string) (*sql.Stmt, error) {
	db.mu.RLock()
	if stmt, exists := db.stmts[key]; exists {
		db.mu.RUnlock()
		<-stmt.done
		if stmt.err != nil {
			return nil, stmt.err
		}
		return stmt.stmt, nil
	}
	db.mu.RUnlock()

	db.mu.Lock()
	if stmt, exists := db.stmts[key]; exists {
		db.mu.Unlock()
		<-stmt.done
		if stmt.err != nil {
			return nil, stmt.err
		}
		return stmt.stmt, nil
	}

	var stmt = &Stmt{done: make(chan struct{})}
	db.stmts[key] = stmt
	db.mu.Unlock()

	defer close(stmt.done)

	nStmt, err := db.db.PrepareContext(ctx, query)
	if err != nil {
		stmt.err = err
		db.mu.Lock()
		delete(db.stmts, key)
		db.mu.Unlock()
		return nil, err
	}
	db.mu.Lock()
	stmt.stmt = nStmt
	db.mu.Unlock()
	return nStmt, nil
}

// RevokeStatement 废弃已缓存的预处理语句(sql.Stmt)。
func (db *DB) RevokeStatement(key string) {
	db.mu.RLock()
	var stmt, exists = db.stmts[key]
	db.mu.RUnlock()

	if exists {
		<-stmt.done
		db.removeStatement(key, stmt.stmt)
	}
}

func (db *DB) removeStatement(key string, stmt *sql.Stmt) {
	db.mu.Lock()
	if stmt != nil {
		go stmt.Close()
	}
	delete(db.stmts, key)
	db.mu.Unlock()
}

// statement 使用参数 query 获取已经缓存的预处理语句(sql.Stmt)。
//
// 两种情况：
//   - 缓存中若存在，则直接返回；
//   - 缓存中不存在，则根据 query 参数创建一个预处理语句并将其缓存；
func (db *DB) statement(ctx context.Context, query string) (*sql.Stmt, error) {
	return db.prepareStatement(ctx, query, query)
}

func (db *DB) Exec(query string, args ...interface{}) (sql.Result, error) {
	return db.ExecContext(context.Background(), query, args...)
}

func (db *DB) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	if _, ok := ctx.Deadline(); !ok && db.execTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, db.execTimeout)
		defer cancel()
	}

	stmt, err := db.statement(ctx, query)
	if err != nil {
		return nil, err
	}
	result, err := stmt.ExecContext(ctx, args...)
	if err != nil {
		db.removeStatement(query, stmt)
	}
	return result, err
}

func (db *DB) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return db.QueryContext(context.Background(), query, args...)
}

func (db *DB) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	if _, ok := ctx.Deadline(); !ok && db.queryTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, db.queryTimeout)
		defer cancel()
	}

	stmt, err := db.statement(ctx, query)
	if err != nil {
		return nil, err
	}
	rows, err := stmt.QueryContext(ctx, args...)
	if err != nil {
		db.removeStatement(query, stmt)
	}
	return rows, err
}

func (db *DB) QueryRow(query string, args ...any) *sql.Row {
	return db.QueryRowContext(context.Background(), query, args...)
}

func (db *DB) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	if _, ok := ctx.Deadline(); !ok && db.queryTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, db.queryTimeout)
		defer cancel()
	}

	stmt, err := db.statement(ctx, query)
	if err != nil {
		return nil
	}
	row := stmt.QueryRowContext(ctx, args...)
	if row.Err() != nil {
		db.removeStatement(query, stmt)
	}
	return row
}

func (db *DB) Close() error {
	return db.db.Close()
}

func (db *DB) Begin() (*Tx, error) {
	return db.BeginTx(context.Background(), nil)
}

func (db *DB) BeginTx(ctx context.Context, opts *sql.TxOptions) (*Tx, error) {
	var cancel context.CancelFunc
	if _, ok := ctx.Deadline(); !ok && db.txTimeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, db.txTimeout)
	} else {
		ctx, cancel = context.WithCancel(ctx)
	}
	tx, err := db.db.BeginTx(ctx, opts)
	if err != nil {
		cancel()
		return nil, err
	}
	var nTx = &Tx{}
	nTx.tx = tx
	nTx.db = db
	nTx.cancel = cancel
	return nTx, nil
}

type Tx struct {
	tx     *sql.Tx
	db     *DB
	cancel context.CancelFunc
}

func (tx *Tx) Tx() *sql.Tx {
	return tx.tx
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
	if _, ok := ctx.Deadline(); !ok && tx.db.prepareTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, tx.db.prepareTimeout)
		defer cancel()
	}
	return tx.tx.PrepareContext(ctx, query)
}

func (tx *Tx) statement(ctx context.Context, query string) (*sql.Stmt, error) {
	var stmt, err = tx.db.statement(ctx, query)
	if err != nil {
		return nil, err
	}
	return tx.tx.StmtContext(ctx, stmt), nil
}

func (tx *Tx) Exec(query string, args ...interface{}) (sql.Result, error) {
	return tx.ExecContext(context.Background(), query, args...)
}

func (tx *Tx) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	if _, ok := ctx.Deadline(); !ok && tx.db.execTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, tx.db.execTimeout)
		defer cancel()
	}

	stmt, err := tx.statement(ctx, query)
	if err != nil {
		return nil, err
	}
	result, err := stmt.ExecContext(ctx, args...)
	if err != nil {
		tx.db.removeStatement(query, stmt)
	}
	return result, err
}

func (tx *Tx) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return tx.QueryContext(context.Background(), query, args...)
}

func (tx *Tx) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	if _, ok := ctx.Deadline(); !ok && tx.db.queryTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, tx.db.queryTimeout)
		defer cancel()
	}

	stmt, err := tx.statement(ctx, query)
	if err != nil {
		return nil, err
	}
	rows, err := stmt.QueryContext(ctx, args...)
	if err != nil {
		tx.db.removeStatement(query, stmt)
	}
	return rows, err
}

func (tx *Tx) QueryRow(query string, args ...any) *sql.Row {
	return tx.QueryRowContext(context.Background(), query, args...)
}

func (tx *Tx) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	if _, ok := ctx.Deadline(); !ok && tx.db.queryTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, tx.db.queryTimeout)
		defer cancel()
	}

	stmt, err := tx.statement(ctx, query)
	if err != nil {
		return nil
	}
	row := stmt.QueryRowContext(ctx, args...)
	if row.Err() != nil {
		tx.db.removeStatement(query, stmt)
	}
	return row
}

func (tx *Tx) ToContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, sessionKey{}, tx)
}

func (tx *Tx) Commit() error {
	var err = tx.tx.Commit()
	tx.cancel()
	return err
}

func (tx *Tx) Rollback() error {
	var err = tx.tx.Rollback()
	tx.cancel()
	return err
}
