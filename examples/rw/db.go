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

func (db *DB) Master() dbs.Database {
	return db.master
}

func (db *DB) Slave() dbs.Database {
	return db.slaves[int(atomic.AddUint32(&db.offset, 1)-1)%db.size]
}

func (db *DB) Close() error {
	db.master.Close()
	for _, slave := range db.slaves {
		slave.Close()
	}
	return nil
}

func (db *DB) Prepare(query string) (*sql.Stmt, error) {
	return db.PrepareContext(context.Background(), query)
}

func (db *DB) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	return db.master.PrepareContext(ctx, query)
}

func (db *DB) Exec(query string, args ...interface{}) (sql.Result, error) {
	return db.ExecContext(context.Background(), query, args...)
}

func (db *DB) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return db.master.ExecContext(ctx, query, args...)
}

func (db *DB) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return db.QueryContext(context.Background(), query, args...)
}

func (db *DB) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	if db.size > 0 {
		var slave = db.slaves[int(atomic.AddUint32(&db.offset, 1)-1)%db.size]
		return slave.QueryContext(ctx, query, args...)
	}
	return db.master.QueryContext(ctx, query, args...)
}

func (db *DB) QueryRow(query string, args ...any) *sql.Row {
	return db.QueryRowContext(context.Background(), query, args...)
}

func (db *DB) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	if db.size > 0 {
		var slave = db.slaves[int(atomic.AddUint32(&db.offset, 1)-1)%db.size]
		return slave.QueryRowContext(ctx, query, args...)
	}
	return db.master.QueryRowContext(ctx, query, args...)
}

func (db *DB) Begin() (*dbs.Tx, error) {
	return db.master.Begin()
}

func (db *DB) BeginTx(ctx context.Context, opts *sql.TxOptions) (*dbs.Tx, error) {
	return db.master.BeginTx(ctx, opts)
}
