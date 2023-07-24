package rw

import (
	"context"
	"database/sql"
	"github.com/smartwalle/dbs"
	"sync/atomic"
)

type DB struct {
	master *dbs.DB
	slaves []*dbs.DB
	size   int
	offset uint32
}

func New(master *dbs.DB, slaves ...*dbs.DB) *DB {
	var ndb = &DB{}
	ndb.master = master
	ndb.slaves = slaves
	ndb.size = len(slaves)
	return ndb
}

func (this *DB) Master() *dbs.DB {
	return this.master
}

func (this *DB) Close() error {
	this.master.Close()
	for _, slave := range this.slaves {
		slave.Close()
	}
	return nil
}

func (this *DB) Prepare(query string) (*sql.Stmt, error) {
	return this.PrepareContext(context.Background(), query)
}

func (this *DB) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	return this.master.PrepareContext(ctx, query)
}

func (this *DB) Exec(query string, args ...interface{}) (sql.Result, error) {
	return this.ExecContext(context.Background(), query, args...)
}

func (this *DB) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return this.master.ExecContext(ctx, query, args...)
}

func (this *DB) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return this.QueryContext(context.Background(), query, args...)
}

func (this *DB) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	if this.size > 0 {
		var slave = this.slaves[int(atomic.AddUint32(&this.offset, 1)-1)%this.size]
		return slave.QueryContext(ctx, query, args...)
	}
	return this.master.QueryContext(ctx, query, args...)
}

func (this *DB) QueryRow(query string, args ...any) *sql.Row {
	return this.QueryRowContext(context.Background(), query, args...)
}

func (this *DB) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	if this.size > 0 {
		var slave = this.slaves[int(atomic.AddUint32(&this.offset, 1)-1)%this.size]
		return slave.QueryRowContext(ctx, query, args...)
	}
	return this.master.QueryRowContext(ctx, query, args...)
}

func (this *DB) Begin() (*dbs.Tx, error) {
	return this.master.Begin()
}

func (this *DB) BeginTx(ctx context.Context, opts *sql.TxOptions) (*dbs.Tx, error) {
	return this.master.BeginTx(ctx, opts)
}
