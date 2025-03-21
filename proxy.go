package dbs

import (
	"context"
	"database/sql"
	"sync/atomic"
)

// Proxy 维护一个 master 节点和多个 slave 节点的数据库连接信息，用于实现读写分离操作。
//
// 执行写（默认为调用 DB.Exec()、Proxy.ExecContext() 方法执行的操作）的操作在 master 节点上执行。
//
// 执行读（默认为调用 Proxy.Query()、DB.QueryContext()、Proxy.QueryRow()、DB.QueryRowContext() 方法执行的操作）的操作默认在 slave 节点上执行。
type Proxy struct {
	master         Database
	slaves         []Database
	numberOfSlaves int
	slaveOffset    uint32
}

type masterKey struct{}

// WithMaster 有一些特殊情况需要在 master 节点执行 Query 相关的操作，可以通过传递 WithMaster() 函数返回的 context.Context
// 用于指定该操作在 master 节点上执行。
//
// 仅 Proxy.QueryContext() 和 DB.QueryRowContext() 支持。
func WithMaster(ctx context.Context) context.Context {
	return context.WithValue(ctx, masterKey{}, true)
}

type slaveKey struct{}

// WithSlave 用于指定操作在 slave 节点上执行。
//
// 仅 Proxy.PrepareContext() 支持。
func WithSlave(ctx context.Context) context.Context {
	return context.WithValue(ctx, slaveKey{}, true)
}

func NewProxy(master Database, slaves ...Database) *Proxy {
	var ndb = &Proxy{}
	ndb.master = master
	ndb.slaves = slaves
	ndb.numberOfSlaves = len(slaves)
	return ndb
}

func (p *Proxy) Master() Database {
	return p.master
}

func (p *Proxy) Slave() Database {
	return p.slaves[int(atomic.AddUint32(&p.slaveOffset, 1)-1)%p.numberOfSlaves]
}

func (p *Proxy) Slaves() []Database {
	return p.slaves
}

func (p *Proxy) Session(ctx context.Context) Session {
	var session, exists = ctx.Value(sessionKey{}).(Session)
	if exists && session != nil {
		return session
	}
	return p
}

func (p *Proxy) Prepare(query string) (*sql.Stmt, error) {
	return p.PrepareContext(context.Background(), query)
}

func (p *Proxy) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	var slave, _ = ctx.Value(slaveKey{}).(bool)
	if slave && p.numberOfSlaves > 0 {
		return p.Slave().PrepareContext(ctx, query)
	}
	return p.master.PrepareContext(ctx, query)
}

func (p *Proxy) Exec(query string, args ...interface{}) (sql.Result, error) {
	return p.ExecContext(context.Background(), query, args...)
}

func (p *Proxy) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return p.master.ExecContext(ctx, query, args...)
}

func (p *Proxy) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return p.QueryContext(context.Background(), query, args...)
}

func (p *Proxy) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	var master, _ = ctx.Value(masterKey{}).(bool)
	if !master && p.numberOfSlaves > 0 {
		return p.Slave().QueryContext(ctx, query, args...)
	}
	return p.master.QueryContext(ctx, query, args...)
}

func (p *Proxy) QueryRow(query string, args ...any) *sql.Row {
	return p.QueryRowContext(context.Background(), query, args...)
}

func (p *Proxy) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	var master, _ = ctx.Value(masterKey{}).(bool)
	if !master && p.numberOfSlaves > 0 {
		return p.Slave().QueryRowContext(ctx, query, args...)
	}
	return p.master.QueryRowContext(ctx, query, args...)
}

func (p *Proxy) Close() error {
	if err := p.master.Close(); err != nil {
		return err
	}
	for _, slave := range p.slaves {
		if err := slave.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (p *Proxy) Begin() (*Tx, error) {
	return p.master.Begin()
}

func (p *Proxy) BeginTx(ctx context.Context, opts *sql.TxOptions) (*Tx, error) {
	return p.master.BeginTx(ctx, opts)
}
