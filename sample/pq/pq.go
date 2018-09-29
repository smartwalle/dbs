package main

import (
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	"github.com/smartwalle/dbs"
	"github.com/smartwalle/time4go"
)

func main() {
	dbs.Placeholder = dbs.Dollar

	db, err := sql.Open("postgres", "user=postgres password=yangfeng dbname=test sslmode=disable")
	if err != nil {
		fmt.Println("连接数据库出错：", err)
		return
	}
	defer db.Close()

	var tx = dbs.MustTx(db)

	var ub = dbs.NewUpdateBuilder()
	ub.SET("email", "smartwalle@gmail.com")
	ub.SET("name", "杨烽")
	ub.SET("updated_on", time4go.Now())
	ub.Table("u_user")
	ub.Where("id = ?", 1)
	if _, err = ub.Exec(tx); err != nil {
		fmt.Println("更新出错：", err)
		return
	}

	var user *User
	var sb = dbs.NewSelectBuilder()
	sb.Selects("u.id", "u.email", "u.name", "u.updated_on")
	sb.From("u_user", "AS u")
	sb.Where("u.id = ?", 1)
	sb.Limit(1)
	if err = sb.Scan(db, &user); err != nil {
		fmt.Println("查询出错：", err)
		return
	}

	tx.Commit()

	if user != nil {
		fmt.Println(user.Id, user.Name, user.Email, user.UpdatedOn)
	}
}

type User struct {
	Id        int64         `sql:"id"`
	Email     string        `sql:"email"`
	Name      string        `sql:"name"`
	UpdatedOn *time4go.Time `sql:"updated_on"`
}
