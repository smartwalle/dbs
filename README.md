## DBS

SQL Builder 工具, 不是 ORM。

## 帮助

在集成的过程中有遇到问题，欢迎加 QQ 群 564704807 讨论。

### 安装
```bash
$ go get github.com/smartwalle/dbs
```

### 开始
```go
package main

import (
	"fmt"
	"github.com/smartwalle/dbs"
)

func main() {
	var sb = dbs.NewSelectBuilder()
	sb.Selects("u.id", "u.name", "u.age")
	sb.From("user", "AS u")
	sb.Where("u.id = ?", 1)
	sb.Limit(1)

	sqlStr, args, _ := sb.ToSQL()
	fmt.Println("sqlStr:", sqlStr)
	fmt.Println("args:", args)
}

```

上述代码会输出如下内容：

```bash
sql: SELECT u.id, u.name, u.age FROM user AS u WHERE u.id = ? LIMIT ?
args: [10 1]
```

#### 执行 SQL

此处使用 MySQL 作为演示，需要准备好一个测试用数据库及 user 表，user 表结构如下：

```sql
CREATE TABLE `user` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(32) DEFAULT NULL,
  `age` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `human_id_uindex` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=utf8

```

```go
package main

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/smartwalle/dbs"
)

func main() {
	var sb = dbs.NewSelectBuilder()
	sb.Selects("u.id", "u.name", "u.age")
	sb.From("user", "AS u")
	sb.Where("u.id = ?", 1)
	sb.Limit(1)

	db, err := sql.Open("mysql", "数据库连接信息")
	if err != nil {
		fmt.Println("连接数据库出错：", err)
		return
	}
	defer db.Close()

	var user *User
	if err := sb.Scan(db, &user); err != nil {
		fmt.Println("Query 出错：", err)
		return
	}

	if user != nil {
		fmt.Println(user.Id, user.Name, user.Age)
	}
}

type User struct {
	Id   int64  `sql:"id"`
	Name string `sql:"name"`
	Age  int    `sql:"age"`
}
```

运行上述代码，如果数据库中存在 id 为 1 的数据，则会正常输出相关的信息。

#### Select

```go
var sb = dbs.NewSelectBuilder()
sb.Selects("u.id", "u.name AS username", "u.age")
sb.Select(dbs.Alias("b.amount", "user_amount"))
sb.From("user", "AS u")
sb.LeftJoin("bank", "AS b ON b.user_id = u.id")
sb.Where("u.id = ?", 1)
fmt.Println(sb.ToSQL())
```

执行 Select 语句：

```go
db, err := sql.Open("mysql", "数据库连接信息")
var user *User
sb.Scan(db, &user)
```

#### Insert

```go
var ib = dbs.NewInsertBuilder()
ib.Table("user")
ib.Columns("name", "age")
ib.Values("用户1", 18)
ib.Values("用户2", 20)
fmt.Println(ib.ToSQL())
```

执行 Insert 语句：

```go
db, err := sql.Open("mysql", "数据库连接信息")
ib.Exec(db)
```

#### Update
```go
var ub = dbs.NewUpdateBuilder()
ub.Table("user")
ub.SET("name", "新的名字")
ub.Where("id = ? ", 1)
ub.Limit(1)
fmt.Println(ub.ToSQL())
```

执行 Update 语句：

```go
db, err := sql.Open("mysql", "数据库连接信息")
ub.Exec(db)
```

#### Delete

```go
var rb = dbs.NewDeleteBuilder()
rb.Table("user")
rb.Where("id = ?", 1)
rb.Limit(1)
fmt.Println(rb.ToSQL())
```

执行 Delete 语句：

```go
db, err := sql.Open("mysql", "数据库连接信息")
rb.Exec(db)
```

更多内容请参考 test 文件。

## License
This project is licensed under the MIT License.