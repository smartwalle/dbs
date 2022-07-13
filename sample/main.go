package main

import (
	"database/sql"
	"fmt"
	//_ "github.com/go-sql-driver/mysql"
	"github.com/smartwalle/dbs"
)

func main() {
	db, err := sql.Open("mysql", "xxx")
	if err != nil {
		fmt.Println("连接数据库出错：", err)
		return
	}
	defer db.Close()

	var sb = dbs.NewSelectBuilder()
	sb.Selects("u.id", "u.name", "u.age")
	sb.From("user", "AS u")
	sb.Where("u.id = ?", 1)
	sb.Limit(1)

	var user *User
	if err := sb.Scan(db, &user); err != nil {
		fmt.Println("Query 出错：", err)
	}

	if user != nil {
		fmt.Println(user.Id, user.Name, user.Age)
	}

	// 事务示例
	//var tx = dbs.MustTx(db)
	//
	//var sb2 = dbs.NewSelectBuilder()
	//if err = sb2.Scan(tx, &user); err != nil {
	//	return
	//}
	//
	//var ib = dbs.NewInsertBuilder()
	//if _, err = ib.Exec(tx); err != nil {
	//	return
	//}
	//
	//var ub = dbs.NewUpdateBuilder()
	//if _, err = ub.Exec(tx); err != nil {
	//	return
	//}
	//
	//var rb = dbs.NewDeleteBuilder()
	//if _, err = rb.Exec(tx); err != nil {
	//	return
	//}
	//
	//tx.Commit()
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
	fmt.Println(sb.SQL())
}

func insertBuilder() {
	var ib = dbs.NewInsertBuilder()
	ib.Table("user")
	ib.Columns("name", "age")
	ib.Values("用户1", 18)
	ib.Values("用户2", 20)
	fmt.Println(ib.SQL())
}

func updateBuilder() {
	var ub = dbs.NewUpdateBuilder()
	ub.Table("user")
	ub.SET("name", "新的名字")
	ub.Where("id = ? ", 1)
	ub.Limit(1)
	fmt.Println(ub.SQL())
}

func deleteBuilder() {
	var rb = dbs.NewDeleteBuilder()
	rb.Table("user")
	rb.Where("id = ?", 1)
	rb.Limit(1)
	fmt.Println(rb.SQL())
}
