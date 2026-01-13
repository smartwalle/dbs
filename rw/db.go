package rw

import (
	"context"
	"database/sql"
	"github.com/smartwalle/dbs"
	"github.com/smartwalle/dbs/internal"
)

// DB 维护一个 master 节点和多个 slave 节点的数据库连接信息，用于实现读写分离操作。
//
//	执行写（默认为调用 ExecContext() 方法执行的操作）的操作在 master 节点上执行。
//
//	执行读（默认为调用 QueryContext()、QueryRowContext() 方法执行的操作）的操作默认在 slave 节点上执行。
type DB struct {
	master dbs.Database
	slave  dbs.Database
}

type masterKey struct{}

// WithMaster 有一些特殊情况需要在 master 节点执行 Query 相关的操作，可以通过传递 WithMaster() 函数返回的 context.Context
//
//	用于指定该操作在 master 节点上执行。
//
// 仅 QueryContext() 和 QueryRowContext() 支持。
func WithMaster(ctx context.Context) context.Context {
	return context.WithValue(ctx, masterKey{}, true)
}

type slaveKey struct{}

// WithSlave 用于指定操作在 slave 节点上执行。
//
//	仅 PrepareContext() 支持。
func WithSlave(ctx context.Context) context.Context {
	return context.WithValue(ctx, slaveKey{}, true)
}

func New(master, slave dbs.Database) *DB {
	var ndb = &DB{}
	ndb.master = master
	ndb.slave = slave
	return ndb
}

func (db *DB) Master() dbs.Database {
	return db.master
}

func (db *DB) Slave() dbs.Database {
	return db.slave
}

func (db *DB) Dialect() dbs.Dialect {
	return db.master.Dialect()
}

func (db *DB) Logger() dbs.Logger {
	return db.master.Logger()
}

func (db *DB) Mapper() dbs.Mapper {
	return db.master.Mapper()
}

func (db *DB) Session(ctx context.Context) dbs.Session {
	var session, ok = ctx.Value(internal.TxSessionKey{}).(dbs.Session)
	if ok && session != nil {
		return session
	}
	return db
}

func (db *DB) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	var slave, _ = ctx.Value(slaveKey{}).(bool)
	if slave && db.slave != nil {
		return db.slave.PrepareContext(ctx, query)
	}
	return db.master.PrepareContext(ctx, query)
}

func (db *DB) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return db.master.ExecContext(ctx, query, args...)
}

func (db *DB) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	var master, _ = ctx.Value(masterKey{}).(bool)
	if master || db.slave == nil {
		return db.master.QueryContext(ctx, query, args...)
	}
	return db.slave.QueryContext(ctx, query, args...)
}

func (db *DB) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	var master, _ = ctx.Value(masterKey{}).(bool)
	if master || db.slave == nil {
		return db.master.QueryRowContext(ctx, query, args...)
	}
	return db.slave.QueryRowContext(ctx, query, args...)
}

func (db *DB) Close() error {
	if db.slave != nil {
		if err := db.slave.Close(); err != nil {
			return err
		}
	}
	return db.master.Close()
}

func (db *DB) Begin(ctx context.Context) (*dbs.Tx, error) {
	return db.master.Begin(ctx)
}

func (db *DB) BeginTx(ctx context.Context, opts *sql.TxOptions) (*dbs.Tx, error) {
	return db.master.BeginTx(ctx, opts)
}
