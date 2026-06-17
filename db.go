package dbs

import (
	"context"
	"database/sql"
	"sync/atomic"
	"unsafe"

	"github.com/smartwalle/dbs/internal"
	"github.com/smartwalle/dbs/logger"
)

var ErrNoRows = sql.ErrNoRows
var ErrTxDone = sql.ErrTxDone

type Session interface {
	Dialect() Dialect

	Logger() Logger
	Mapper() Mapper

	Preparer

	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)

	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)

	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

type Preparer interface {
	PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)
}

type Database interface {
	Session

	Close() error

	Session(ctx context.Context) Session

	Begin(ctx context.Context) (*Tx, error)
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*Tx, error)
}

func Open(driver, url string, maxOpen, maxIdle int) (*DB, error) {
	db, err := sql.Open(driver, url)
	if err != nil {
		return nil, err
	}

	if err = db.Ping(); err != nil {
		_ = db.Close()
		return nil, err
	}

	db.SetMaxIdleConns(maxIdle)
	db.SetMaxOpenConns(maxOpen)
	return New(db), err
}

type DB struct {
	db      unsafe.Pointer
	dialect Dialect
	logger  Logger
	mapper  Mapper
}

func New(db *sql.DB) *DB {
	var ndb = &DB{}
	ndb.UseDB(db)
	ndb.UseLogger(logger.New())
	ndb.UseMapper(NewMapper(kTagSQL))
	return ndb
}

// UseDB 替换底层的 *sql.DB。
//
// ndb 不能为 nil。替换已有数据库时，调用方应该先获取旧的 *sql.DB，
// 并在替换后延迟关闭，尽量确保旧连接上的相关操作已经完成：
//
//	oldDB := db.DB()
//	db.UseDB(newDB)
//	time.AfterFunc(2*time.Second, func() {
//		_ = oldDB.Close()
//	})
func (db *DB) UseDB(ndb *sql.DB) {
	atomic.StorePointer(&db.db, unsafe.Pointer(ndb))
}

func (db *DB) DB() *sql.DB {
	return (*sql.DB)(atomic.LoadPointer(&db.db))
}

func (db *DB) Ping() error {
	return db.DB().Ping()
}

func (db *DB) PingContext(ctx context.Context) error {
	return db.DB().PingContext(ctx)
}

func (db *DB) Dialect() Dialect {
	return db.dialect
}

func (db *DB) UseDialect(dialect Dialect) {
	if dialect != nil {
		db.dialect = dialect
	}
}

func (db *DB) Logger() Logger {
	return db.logger
}

func (db *DB) UseLogger(logger Logger) {
	db.logger = logger
}

func (db *DB) Mapper() Mapper {
	return db.mapper
}

func (db *DB) UseMapper(mapper Mapper) {
	if mapper != nil {
		db.mapper = mapper
	}
}

func (db *DB) Session(ctx context.Context) Session {
	var session, ok = ctx.Value(internal.TxSessionKey{}).(Session)
	if ok && session != nil {
		return session
	}
	return db
}

func (db *DB) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	return db.DB().PrepareContext(ctx, query)
}

func (db *DB) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return db.DB().ExecContext(ctx, query, args...)
}

func (db *DB) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return db.DB().QueryContext(ctx, query, args...)
}

func (db *DB) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return db.DB().QueryRowContext(ctx, query, args...)
}

func (db *DB) Close() error {
	return db.DB().Close()
}

func (db *DB) Begin(ctx context.Context) (*Tx, error) {
	return db.BeginTx(ctx, nil)
}

func (db *DB) BeginTx(ctx context.Context, opts *sql.TxOptions) (*Tx, error) {
	tx, err := db.DB().BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	var nTx = &Tx{}
	nTx.tx = tx
	nTx.db = db
	return nTx, nil
}
