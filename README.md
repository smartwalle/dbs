## DBA

Golang 数据库操作工具集, 目前只提供了一个将 sql.Rows 映射到 Struct 的工具。


#### Bind

适用于开发者自己拼写 SQL 的场景, 可以很简单地将查询数据映射到 Struct。

Bind(rows *sql.Rows, result interface{}) (err error)

```
type Human struct {
	Id     int     `sql:"id"`
	Name   string  `sql:"name"`
	Gender int     `sql:"gender"`
}
```

映射单条数据
```
var rows, _ = sql.Open(...).Query("SELECT id, name, sex as gender FROM human where id = ?", 1)
var h *Human
Bind(rows, &h)

```

映射多条数据
```
var rows, _ = sql.Open(...).Query("SELECT id, name, gender FROM human where id > ? ", 1)
var hList []*Human
Bind(rows, &hList)
```