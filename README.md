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

### 结果映射规则

`dbs` 用结构体字段上的 `sql` 标签把列和字段对应起来：

```go
type User struct {
    Id      int64     `sql:"id"`
    Name    string    `sql:"name"`
    Nick    *string   `sql:"nick"`         // 可空列
    Created time.Time `sql:"created_at"`
    Profile *Profile                         // 没有标签的结构体，字段会展开
    Remark  string    `sql:"remark;default"` // 写入时值是零值，由数据库填默认值
    Ignore  string    `sql:"-"`              // 不参与映射
}
```

几个要注意的地方：

* **可空列对应到指针类型**（`*string`、`*time.Time` 等）。
  指针能接住 `NULL`，解码最快，也不会把「NULL」和「零值」混在一起。
  值类型遇到 `NULL` 会解成零值，但要走兼容路径重扫这一行，慢 3 倍多，
  详见「可空列要用指针类型」。
* **`default` 标签**只影响写入：字段值是零值时由数据库填列的默认值
  （`Repository.Create` 会跳过这个字段，自己拼 `INSERT` 时值写成 `DEFAULT`）；
  解码时忽略这个标记。
* **嵌套结构体**：没有 `sql` 标签的结构体和结构体指针会展开，
  用里面的字段当列名；路径中间的指针是 `nil` 时会自动分配。
* **实现了 `sql.Scanner` 的字段**逐行新建实例，
  字段内部的状态不会在多行之间串行。
* **`[]byte` 和 `sql.RawBytes` 字段**每一行都是独立的数据，不会被下一行覆盖。
* 解码出来的数据是**拷贝**，不和内部复用的扫描目标共享内存，
  查出来的 A 不会被后一次查询 B 改写。
* **解码失败时目标完全不变**：目标切片的长度、内容、底层数组，
  目标 map 的内容，目标结构体的字段，都保持原样；
  目标本来是 nil 指针的，失败之后还是 nil，
  「没查到」和「查询失败」分得清。
* **复用同一个结构体解码**时，这次没查到的字段会被清零，
  不会留着上一次的旧值；
  `sql:"-"` 的字段和没有标签的字段不参与映射，保持原样。
* 解到**单个**基础类型时，列是 `NULL` 会保持目标原来的值（不写零值）；
  解到**切片**时则是零值，并且这一行会保留。

#### 可空列要用指针类型

`NULL` 落在值类型字段上时 `database/sql` 会报错，
`dbs` 只能改用双层指针把这行重扫一遍，`NULL` 取零值。
重扫要多扫一次，且每行每列都要新建一个实例。

1000 行 3 列、2/3 的行该列为 `NULL` 的实测：

| 字段类型 | 耗时 | 分配次数 |
| --- | --- | --- |
| `time.Time` | 296µs | 6,021 |
| `*time.Time` | 94µs | 360 |

可空列一律用指针：

```go
type User struct {
    Id       int64      `sql:"id"`
    Name     string     `sql:"name"`
    BirthDay *time.Time `sql:"birth_day"`
}
```

`map` 的值类型固定，接不住 `NULL`：`map[string]int64` 遇到 `NULL` 直接报错，
有 `NULL` 的列用 `map[string]any`，`NULL` 解成 `nil`。

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
    Id   int    `sql:"id;default"`
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
