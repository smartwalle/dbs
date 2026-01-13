package dbs

import (
	"context"
	"database/sql"
)

// Proxy 维护一个 master 节点和多个 slave 节点的数据库连接信息，用于实现读写分离操作。
//
// 执行写（默认为调用 DB.Exec()、Proxy.ExecContext() 方法执行的操作）的操作在 master 节点上执行。
//
// 执行读（默认为调用 Proxy.Query()、DB.QueryContext()、Proxy.QueryRow()、DB.QueryRowContext() 方法执行的操作）的操作默认在 slave 节点上执行。
type Proxy struct {
	master      Database
	slave       Database
	slaveOffset uint32
	dialect     Dialect
	logger      Logger
	mapper      Mapper
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

func NewProxy(master, slave Database) *Proxy {
	var ndb = &Proxy{}
	ndb.master = master
	ndb.slave = slave
	ndb.logger = NewLogger()
	ndb.mapper = NewMapper(kTagSQL)
	return ndb
}

func (p *Proxy) Master() Database {
	return p.master
}

func (p *Proxy) Slave() Database {
	return p.slave
}

func (p *Proxy) Dialect() Dialect {
	return p.dialect
}

func (p *Proxy) UseDialect(dialect Dialect) {
	if dialect != nil {
		p.dialect = dialect
	}
}

func (p *Proxy) Logger() Logger {
	return p.logger
}

func (p *Proxy) UseLogger(logger Logger) {
	if logger != nil {
		p.logger = logger
	}
}

func (p *Proxy) Mapper() Mapper {
	return p.mapper
}

func (p *Proxy) UseMapper(mapper Mapper) {
	if mapper != nil {
		p.mapper = mapper
	}
}

func (p *Proxy) Session(ctx context.Context) Session {
	var session, ok = ctx.Value(txSessionKey{}).(Session)
	if ok && session != nil {
		return session
	}
	return p
}

func (p *Proxy) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	var slave, _ = ctx.Value(slaveKey{}).(bool)
	if slave && p.slave != nil {
		return p.slave.PrepareContext(ctx, query)
	}
	return p.master.PrepareContext(ctx, query)
}

func (p *Proxy) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return p.master.ExecContext(ctx, query, args...)
}

func (p *Proxy) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	var master, _ = ctx.Value(masterKey{}).(bool)
	if master {
		return p.master.QueryContext(ctx, query, args...)
	}
	return p.slave.QueryContext(ctx, query, args...)
}

func (p *Proxy) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	var master, _ = ctx.Value(masterKey{}).(bool)
	if master {
		return p.master.QueryRowContext(ctx, query, args...)
	}
	return p.slave.QueryRowContext(ctx, query, args...)
}

func (p *Proxy) Close() error {
	if err := p.master.Close(); err != nil {
		return err
	}
	if err := p.slave.Close(); err != nil {
		return err
	}
	return nil
}

func (p *Proxy) Begin(ctx context.Context) (*Tx, error) {
	return p.master.Begin(ctx)
}

func (p *Proxy) BeginTx(ctx context.Context, opts *sql.TxOptions) (*Tx, error) {
	return p.master.BeginTx(ctx, opts)
}
