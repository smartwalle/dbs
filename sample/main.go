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

	db, err := sql.Open("mysql", "xxx")
	if err != nil {
		fmt.Println("连接数据库出错：", err)
		return
	}
	defer db.Close()

	var user *User
	if err := sb.Scan(db, &user); err != nil {
		fmt.Println("Query 出错：", err)
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

func selectBuilder() {
	var sb = dbs.NewSelectBuilder()
	sb.Selects("u.id", "u.name AS username", "u.age")
	sb.Select(dbs.Alias("b.amount", "user_amount"))
	sb.From("user", "AS u")
	sb.LeftJoin("bank", "AS b ON b.user_id = u.id")
	sb.Where("u.id = ?", 1)
	fmt.Println(sb.ToSQL())
}

func insertBuilder() {
	var ib = dbs.NewInsertBuilder()
	ib.Table("user")
	ib.Columns("name", "age")
	ib.Values("用户1", 18)
	ib.Values("用户2", 20)
	fmt.Println(ib.ToSQL())
}

func updateBuilder() {
	var ub = dbs.NewUpdateBuilder()
	ub.Table("user")
	ub.SET("name", "新的名字")
	ub.Where("id = ? ", 1)
	ub.Limit(1)
	fmt.Println(ub.ToSQL())
}

func deleteBuilder() {
	var rb = dbs.NewDeleteBuilder()
	rb.Table("user")
	rb.Where("id = ?", 1)
	rb.Limit(1)
	fmt.Println(rb.ToSQL())
}
