## DBA

Golang 数据库操作工具集。


#### Bind

适用于开发者自己拼写 SQL 的场景, 将查询结果映射到 Struct。

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
var db, _ = sql.Open(...) 
var rows, _ = db.Query("SELECT id, name, gender FROM human where id = ? ", 1)
var h *Human
Bind(rows, &h)

```

映射多条数据
```
var db, _ = sql.Open(...) 
var rows, _ = db.Query("SELECT id, name, gender FROM human where id > ? ", 1)
var hList []*Human
Bind(rows, &hList)
```