package dbs

import (
	"context"
	"database/sql"
	"sync"
)

type TX interface {
	DB

	// Commit 提交事务
	Commit() (err error)

	// Rollback 回滚事务
	Rollback() error

	Stmt(stmt *sql.Stmt) *sql.Stmt

	StmtContext(ctx context.Context, stmt *sql.Stmt) *sql.Stmt
}

type dbsTx struct {
	*sql.Tx
	db   DB
	done bool
	mu   sync.Mutex
}

func (this *dbsTx) Prepare(query string) (*sql.Stmt, error) {
	var stmt, err = this.db.Prepare(query)
	if err != nil {
		return nil, err
	}
	return this.Stmt(stmt), nil
}

func (this *dbsTx) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	var stmt, err = this.db.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	return this.StmtContext(ctx, stmt), nil
}

func (this *dbsTx) Commit() (err error) {
	this.mu.Lock()
	err = this.Tx.Commit()
	this.done = true
	this.mu.Unlock()
	return err
}

func (this *dbsTx) Rollback() (err error) {
	this.mu.Lock()
	err = this.Tx.Rollback()
	this.done = true
	this.mu.Unlock()
	return err
}

// Close 判断事务是否完成，如果未完成，则执行 Rollback 操作
func (this *dbsTx) Close() (err error) {
	this.mu.Lock()
	if this.done {
		this.mu.Unlock()
		return
	}
	err = this.Tx.Rollback()
	this.done = true
	this.mu.Unlock()
	return err
}

func (this *dbsTx) Ping() error {
	return this.db.Ping()
}

func (this *dbsTx) PingContext(ctx context.Context) error {
	return this.db.PingContext(ctx)
}

// 以下几个方法是为了实现 DB 接口，尽量不要使用

// Begin 不会创建新的事务，如果当前事务已经关闭，则会返回事务已结束的错误，如果事务没有关闭，则返回当前事务
func (this *dbsTx) Begin() (*sql.Tx, error) {
	this.mu.Lock()
	defer this.mu.Unlock()

	if this.done {
		return nil, sql.ErrTxDone
	}
	return this.Tx, nil
}

// BeginTx 不会创建新的事务，如果当前事务已经关闭，则会返回事务已结束的错误，如果事务没有关闭，则返回当前事务
func (this *dbsTx) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	this.mu.Lock()
	defer this.mu.Unlock()

	if this.done {
		return nil, sql.ErrTxDone
	}
	return this.Tx, nil
}

// 以上几个方法是为了实现 DB 接口，尽量不要使用

func NewTx(db DB) (TX, error) {
	return newTxContext(context.Background(), db, nil)
}

func MustTx(db DB) TX {
	tx, err := newTxContext(context.Background(), db, nil)
	if err != nil {
		panic(err)
	}
	return tx
}

func NewTxContext(ctx context.Context, db DB, opts *sql.TxOptions) (tx TX, err error) {
	return newTxContext(ctx, db, opts)
}

func MustTxContext(ctx context.Context, db DB, opts *sql.TxOptions) TX {
	tx, err := newTxContext(ctx, db, opts)
	if err != nil {
		panic(err)
	}
	return tx
}

func newTxContext(ctx context.Context, db DB, opts *sql.TxOptions) (TX, error) {
	if nt, ok := db.(*dbsTx); ok {
		nt.mu.Lock()
		if nt.done {
			nt.mu.Unlock()
			return nil, sql.ErrTxDone
		}
		nt.mu.Unlock()
		return nt, nil
	}

	var tx = &dbsTx{}
	var err error

	tx.Tx, err = db.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	tx.db = db
	return tx, err
}
