package dbs

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync/atomic"
	"time"
)

// --------------------------------------------------------------------------------
type TX interface {
	DB

	Stmt(stmt *sql.Stmt) *sql.Stmt
	StmtContext(ctx context.Context, stmt *sql.Stmt) *sql.Stmt

	Commit() (err error)
	Rollback() error
}

// --------------------------------------------------------------------------------
type dbsTx struct {
	id    string
	db    DB
	cache bool
	tx    *sql.Tx
	done  uint32
}

func (this *dbsTx) Tx() *sql.Tx {
	return this.tx
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

func (this *dbsTx) Stmt(stmt *sql.Stmt) *sql.Stmt {
	return this.tx.Stmt(stmt)
}

func (this *dbsTx) StmtContext(ctx context.Context, stmt *sql.Stmt) *sql.Stmt {
	return this.tx.StmtContext(ctx, stmt)
}

func (this *dbsTx) Exec(query string, args ...interface{}) (result sql.Result, err error) {
	if this.cache {
		stmt, err := this.Prepare(query)
		if err != nil {
			return nil, err
		}
		return stmt.Exec(args...)
	}
	return this.tx.Exec(query, args...)
}

func (this *dbsTx) ExecContext(ctx context.Context, query string, args ...interface{}) (result sql.Result, err error) {
	if this.cache {
		stmt, err := this.PrepareContext(ctx, query)
		if err != nil {
			return nil, err
		}
		return stmt.ExecContext(ctx, args...)
	}
	return this.tx.ExecContext(ctx, query, args...)
}

func (this *dbsTx) Query(query string, args ...interface{}) (rows *sql.Rows, err error) {
	if this.cache {
		stmt, err := this.Prepare(query)
		if err != nil {
			return nil, err
		}
		return stmt.Query(args...)
	}
	return this.tx.Query(query, args...)
}

func (this *dbsTx) QueryContext(ctx context.Context, query string, args ...interface{}) (rows *sql.Rows, err error) {
	if this.cache {
		stmt, err := this.PrepareContext(ctx, query)
		if err != nil {
			return nil, err
		}
		return stmt.QueryContext(ctx, args...)
	}
	return this.tx.QueryContext(ctx, query, args...)
}

func (this *dbsTx) Commit() (err error) {
	if err = this.tx.Commit(); err != nil {
		logger.Output(2, fmt.Sprintf("Transaction [%s] Commit Failed: %s \n", this.id, err))
	} else {
		logger.Output(2, fmt.Sprintf("Transaction [%s] Commit Successfully\n", this.id))
	}
	atomic.StoreUint32(&this.done, 1)
	return err
}

func (this *dbsTx) Rollback() (err error) {
	if err = this.tx.Rollback(); err != nil {
		logger.Output(2, fmt.Sprintf("Transaction [%s] Rollback Failed: %s \n", this.id, err))
	} else {
		logger.Output(2, fmt.Sprintf("Transaction [%s] Rollback Successfully\n", this.id))
	}
	atomic.StoreUint32(&this.done, 1)
	return err
}

// 以下几个方法纯粹是为了实现 DB 接口，尽量不要使用

func (this *dbsTx) Close() (err error) {
	return this.Rollback()
}

func (this *dbsTx) Ping() error {
	return this.db.Ping()
}

func (this *dbsTx) PingContext(ctx context.Context) error {
	return this.db.PingContext(ctx)
}

func (this *dbsTx) Begin() (*sql.Tx, error) {
	if this.isDone() {
		return nil, sql.ErrTxDone
	}
	return this.tx, nil
}

func (this *dbsTx) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	if this.isDone() {
		return nil, sql.ErrTxDone
	}
	return this.tx, nil
}

func (this *dbsTx) isDone() bool {
	return atomic.LoadUint32(&this.done) == 1
}

// --------------------------------------------------------------------------------
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

func newTxContext(ctx context.Context, db DB, opts *sql.TxOptions) (TX, error) {
	if nt, ok := db.(*dbsTx); ok {
		if nt.isDone() {
			return nil, sql.ErrTxDone
		}
		return nt, nil
	}

	var tx = &dbsTx{}
	var err error

	tx.tx, err = db.BeginTx(ctx, opts)
	if err != nil {
		logger.Output(3, fmt.Sprintln("Transaction Begin Failed:", err))
		return nil, err
	}
	tx.db = db
	tx.id = genTxId() // 目前只有日志会用到 id
	logger.Output(3, fmt.Sprintf("Transaction [%s] Begin Successfully\n", tx.id))
	_, tx.cache = db.(*DBCache)
	return tx, err
}

func MustTxContext(ctx context.Context, db DB, opts *sql.TxOptions) TX {
	tx, err := newTxContext(ctx, db, opts)
	if err != nil {
		panic(err)
	}
	return tx
}

// --------------------------------------------------------------------------------
func genTxId() string {
	var b = make([]byte, 9)
	binary.BigEndian.PutUint32(b[:], uint32(time.Now().Unix()))

	b[4] = processId[0]
	b[5] = processId[1]

	i := atomic.AddUint32(&idCounter, 1)
	b[6] = byte(i >> 16)
	b[7] = byte(i >> 8)
	b[8] = byte(i)
	return fmt.Sprintf(`%X`, string(b))
}

var idCounter = readRandomUint32()
var processId = readProcessId()

func readRandomUint32() uint32 {
	var b [4]byte
	_, err := io.ReadFull(rand.Reader, b[:])
	if err != nil {
		panic(fmt.Errorf("cannot read random id: %v", err))
	}
	return (uint32(b[0]) << 0) | (uint32(b[1]) << 8) | (uint32(b[2]) << 16) | (uint32(b[3]) << 24)
}

func readProcessId() []byte {
	var pId = os.Getpid()
	var id = make([]byte, 2)
	id[0] = byte(pId >> 8)
	id[1] = byte(pId)
	return id
}
