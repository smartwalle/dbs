# DBS

`dbs` 是一个轻量级的 Go 数据库辅助库：构建 SQL、映射查询结果、管理事务。

## 特性

* **SQL 构建器**：SELECT、INSERT、UPDATE、DELETE
* **结果映射**：结构体（支持嵌套）、Map、基础类型
* **事务**：支持 Context，多个 Repository 共享同一个事务
* **Repository**：通用的增删改查实现
* **可扩展**：自定义 Logger、Dialect、Mapper 标签

## 安装

```bash
go get -u github.com/smartwalle/dbs
```

## 快速开始

### 初始化连接

```go
import (
    "github.com/smartwalle/dbs"
    "github.com/smartwalle/dbs/dialect/postgres"
    _ "github.com/lib/pq" // 导入驱动
)

db, err := dbs.Open("postgres", "host=127.0.0.1 port=5432 user=postgres password=postgres dbname=test sslmode=disable", 10, 10)
if err != nil {
    panic(err)
}
defer db.Close()

db.UseDialect(postgres.Dialect())
```

### 构建 SQL

四个构建器用法一致，都用 `.SQL()` 取 SQL 和参数：

```go
var sb = dbs.NewSelectBuilder()
sb.Selects("id", "name").From("user").Where("age > ?", 18).OrderBy("id DESC").Limit(10)

var ib = dbs.NewInsertBuilder()
ib.Table("user").Columns("name", "age").Values("smartwalle", 20)

var ub = dbs.NewUpdateBuilder()
ub.Table("user").Set("name", "smart").Where("id = ?", 1)

var rb = dbs.NewDeleteBuilder()
rb.Table("user").Where("id = ?", 1)

query, args, err := sb.SQL()
```

### 执行

执行前用 `.UseSession()` 传入会话，会话可以是 `*dbs.DB` 或 `*dbs.Tx`：

```go
// 查询并映射到结构体
type User struct {
    Id   int    `sql:"id"`
    Name string `sql:"name"`
    Age  int    `sql:"age"`
}

var user *User
err := dbs.NewSelectBuilder().
    Selects("*").From("user").Where("id = ?", 1).
    UseSession(db).
    Scan(context.Background(), &user)

// 写入
result, err := dbs.NewUpdateBuilder().
    Table("user").Set("name", "smart").Where("id = ?", 1).
    UseSession(db).
    Exec(context.Background())
```

### 映射规则

`dbs` 用字段上的 `sql` 标签把列和字段对应起来：

```go
type User struct {
    Id      int64     `sql:"id"`
    Name    string    `sql:"name"`
    Nick    *string   `sql:"nick"`           // 可空列
    Created time.Time `sql:"created_at"`
    Profile *Profile                        // 没有标签的结构体，字段会展开
    Remark  string    `sql:"remark;default"` // 写入时零值交给数据库填默认值
    Ignore  string    `sql:"-"`              // 不参与映射
}
```

几条规则：

* **可空列用指针类型**（`*string`、`*time.Time`）。指针能接住 `NULL`，解码最快；
  值类型遇到 `NULL` 要把整行重扫一遍，慢 3 倍多。
* **嵌套结构体**没有 `sql` 标签时会展开，用里面的字段当列名；
  路径中间的指针是 `nil` 时自动分配。
* **`default` 标签**只影响写入：字段值是零值时写 `DEFAULT`（`Repository.Create` 会跳过该字段）；
  解码时忽略。
* **实现了 `sql.Scanner` 的字段**逐行新建实例，状态不会在行之间串；
  列是 `NULL` 时值类型字段把 `nil` 交给 Scanner（和 `database/sql` 一致），指针类型字段置 `nil`。
* 解出来的数据是**拷贝**，每行独立，不和内部复用的扫描目标共享内存，
  查出来的 A 不会被后一次查询 B 改写（`[]byte` 和 `sql.RawBytes` 字段也一样）。
* **解码失败时目标完全不变**：切片、map、结构体都保持原样；
  目标本来是 nil 指针的，失败后还是 nil，「没查到」和「查询失败」分得清。
* **复用同一个结构体解码**时，这次没查到的字段会被清零；
  `sql:"-"` 和没有标签的字段保持原样。

可空列对应到值类型时的实测（1000 行 3 列，2/3 的行该列为 `NULL`）：

| 字段类型 | 耗时 | 分配次数 |
| --- | --- | --- |
| `time.Time` | 296µs | 6,021 |
| `*time.Time` | 94µs | 360 |

`map` 的值类型固定，接不住 `NULL`：`map[string]int64` 遇到 `NULL` 直接报错，
有 `NULL` 的列用 `map[string]any`，`NULL` 解成 `nil`。

### 事务

`Begin` 开启事务，`Commit` / `Rollback` 结束。
把事务放进 `Context`，构建器用 `db.Session(ctx)` 就能自动参与：

```go
tx, err := db.Begin(ctx)
if err != nil {
    return err
}
defer tx.Rollback()

ctx = dbs.WithSession(ctx, tx)
// 用 db.Session(ctx) 执行的操作都在这个事务里
if err = doSomething(ctx); err != nil {
    return err
}
return tx.Commit()
```

Repository 自带事务封装，出错自动回滚：

```go
err := userRepo.Transaction(ctx, func(ctx context.Context) error {
    // 里面的操作都在同一个事务里
    return nil
})
```

### Repository

```go
// 1. 定义实体
type User struct {
    Id   int    `sql:"id;default"`
    Name string `sql:"name"`
}

func (u User) TableName() string   { return "user" }
func (u User) PrimaryKey() string { return "id" }

// 2. 创建 Repository
var userRepo = dbs.NewRepository[User](db)

// 3. 基础操作
user, err := userRepo.Find(context.Background(), 1, "*")
result, err := userRepo.Create(context.Background(), &User{Name: "test"})
```

## 更多示例

见 [examples](examples) 目录。

## 许可证

MIT，见 [LICENSE](LICENSE)。
