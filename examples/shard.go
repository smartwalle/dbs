package main

import (
	"context"
	"fmt"
	_ "github.com/lib/pq"
	"github.com/smartwalle/dbs"
	"log/slog"
)

func main() {
	db1, err := dbs.Open("postgres", "host=127.0.0.1 port=5432 user=postgres password=postgres dbname=db1 sslmode=disable", 10, 1)
	if err != nil {
		slog.Error("连接数据库发生错误", slog.Any("error", err))
		return
	}

	db2, err := dbs.Open("postgres", "host=127.0.0.1 port=5432 user=postgres password=postgres dbname=db2 sslmode=disable", 10, 1)
	if err != nil {
		slog.Error("连接数据库发生错误", slog.Any("error", err))
		return
	}

	var shard = dbs.NewShard(func(value interface{}) int {
		return value.(int)
	}, dbs.New(db1), dbs.New(db2))

	for i := 0; i < 10; i++ {
		shard.ExecContext(dbs.WithShardValue(context.Background(), i%2), "INSERT INTO public.user VALUES($1)", fmt.Sprintf("m%d@qq.com", i))
	}
}
