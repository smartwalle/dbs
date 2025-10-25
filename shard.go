package dbs

import (
	"context"
	"database/sql"
)

// Shard 维护一组数据库连接信息，用于实现简单的分片操作。
type Shard struct {
	sharding       func(value interface{}) uint32
	shards         []Database
	numberOfShards int
}

type shardKey struct{}

// WithShardValue 设定一个值，该值会传递到 sharding 函数中，用于分片节点选举计算。
func WithShardValue(ctx context.Context, value interface{}) context.Context {
	return context.WithValue(ctx, shardKey{}, value)
}

func NewShard(sharding func(value interface{}) uint32, shards ...Database) *Shard {
	var ndb = &Shard{}
	ndb.sharding = sharding
	ndb.shards = shards
	ndb.numberOfShards = len(shards)
	return ndb
}

func (s *Shard) Shard(ctx context.Context) Database {
	var value = ctx.Value(shardKey{})
	var idx = s.sharding(value)
	return s.shards[idx]
}

func (s *Shard) Shards() []Database {
	return s.shards
}

func (s *Shard) Session(ctx context.Context) Session {
	var session, ok = ctx.Value(sessionKey{}).(Session)
	if ok && session != nil {
		return session
	}
	return s
}

func (s *Shard) Prepare(query string) (*sql.Stmt, error) {
	return s.PrepareContext(context.Background(), query)
}

func (s *Shard) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	return s.Shard(ctx).PrepareContext(ctx, query)
}

func (s *Shard) Exec(query string, args ...interface{}) (sql.Result, error) {
	return s.ExecContext(context.Background(), query, args...)
}

func (s *Shard) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return s.Shard(ctx).ExecContext(ctx, query, args...)
}

func (s *Shard) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return s.QueryContext(context.Background(), query, args...)
}

func (s *Shard) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return s.Shard(ctx).QueryContext(ctx, query, args...)
}

func (s *Shard) QueryRow(query string, args ...any) *sql.Row {
	return s.QueryRowContext(context.Background(), query, args...)
}

func (s *Shard) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return s.Shard(ctx).QueryRowContext(ctx, query, args...)
}

func (s *Shard) Close() error {
	for _, db := range s.shards {
		if err := db.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (s *Shard) Begin() (*Tx, error) {
	return s.BeginTx(context.Background(), nil)
}

func (s *Shard) BeginTx(ctx context.Context, opts *sql.TxOptions) (*Tx, error) {
	return s.Shard(ctx).BeginTx(ctx, opts)
}
