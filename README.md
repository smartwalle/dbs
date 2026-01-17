# DBS

`dbs` 是一个轻量级的 Go 语言数据库操作辅助库，旨在简化 SQL 构建、结果集映射以及数据库事务管理。它提供了一个直观的内部 DSL 来构建复杂的 SQL 查询，并能自动将查询结果映射到 Go 结构体、Map 或基础类型。

## 特性

*   **直观的 SQL 构建器**：支持 SELECT, INSERT, UPDATE, DELETE 语句构建。
*   **结果集映射 (Mapper)**：自动将 `sql.Rows` 映射到结构体（支持嵌套）、Map 以及切片。
*   **事务管理**：支持简单的事务封装和 Context 集成，方便在多个 Repository 间共享事务。
*   **Repository 模式**：内置通用的 Repository 实现，快速实现基础的增删改查。
*   **灵活的扩展性**：支持自定义 Logger、Dialect（方言）和 Mapper 标签。

## 安装

```bash
go get -u github.com/smartwalle/dbs
```

## 快速开始

### 初始化数据库连接

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

// 设置方言（Dialect）
db.UseDialect(postgres.Dialect())
```

### 使用 SQL 构建器

`dbs` 提供了一组构建器，用于生成 SQL 语句。所有构建器都支持通过 `.SQL()` 方法获取生成的 SQL 字符串和参数。

#### 查询 (SELECT)

```go
var sb = dbs.NewSelectBuilder()
sb.Selects("id", "name", "age")
sb.From("user")
sb.Where("age > ?", 18)
sb.OrderBy("id DESC")
sb.Limit(10)

// 获取构建好的 SQL 和参数
query, args, err := sb.SQL()
```

#### 插入 (INSERT)

```go
var ib = dbs.NewInsertBuilder()
ib.Table("user")
ib.Columns("name", "age")
ib.Values("smartwalle", 20)

// 获取构建好的 SQL 和参数
query, args, err := ib.SQL()
```

#### 更新 (UPDATE)

```go
var ub = dbs.NewUpdateBuilder()
ub.Table("user")
ub.Set("name", "new_name")
ub.Set("age", 25)
ub.Where("id = ?", 1)

// 获取构建好的 SQL 和参数
query, args, err := ub.SQL()
```

#### 删除 (DELETE)

```go
var rb = dbs.NewDeleteBuilder()
rb.Table("user")
rb.Where("id = ?", 1)

// 获取构建好的 SQL 和参数
query, args, err := rb.SQL()
```

### 执行 SQL 和结果映射

在执行 `Exec` 或 `Scan` 之前，必须先通过 `.UseSession()` 方法为构建器设置一个会话（`Session`），该会话可以是 `*dbs.DB` 或 `*dbs.Tx`。

#### 执行更新操作 (INSERT/UPDATE/DELETE)

```go
var ub = dbs.NewUpdateBuilder()
ub.Table("user")
ub.Set("name", "smart")
ub.Where("id = ?", 1)

// 设置 Session 并执行
result, err := ub.UseSession(db).Exec(context.Background())
```

#### 执行查询并映射结果 (SELECT)

```go
type User struct {
    Id   int    `sql:"id"`
    Name string `sql:"name"`
    Age  int    `sql:"age"`
}

var user *User
var sb = dbs.NewSelectBuilder()
sb.Selects("*").From("user").Where("id = ?", 1)

// 设置 Session 并自动映射到结构体
err := sb.UseSession(db).Scan(context.Background(), &user)
```

### 事务管理

`dbs` 支持通过 `Transaction` 方法方便地处理事务：

```go
err := db.Transaction(context.Background(), func(ctx context.Context) error {
    // 在事务中执行操作
    // 所有的构建器只需 UseSession(db.Session(ctx)) 即可自动参与到当前事务中
    return nil
})
```

### 使用 Repository 模式

```go
// 1. 定义实体
type User struct {
    Id   int    `sql:"id"`
    Name string `sql:"name"`
}

func (u *User) TableName() string { return "user" }
func (u *User) PrimaryKey() string { return "id" }

// 2. 创建 Repository
var userRepo = dbs.NewRepository[User](db)

// 3. 基础操作
user, err := userRepo.Find(context.Background(), 1, "*")
result, err := userRepo.Create(context.Background(), &User{Name: "test"})
```

## 更多示例

请参考 [examples](examples) 目录下的详细示例。

## 许可证
本项目采用 MIT 许可证。详见 [LICENSE](LICENSE) 文件。