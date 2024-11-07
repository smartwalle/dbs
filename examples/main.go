package main

import (
	"context"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	"github.com/smartwalle/dbs"
	"os"
	"time"
)

type Mail struct {
	Id        string     `sql:"id"`
	Email     string     `sql:"email"`
	Status    string     `sql:"status"`
	CreatedAt *time.Time `sql:"created_at"`
	UpdatedAt *time.Time `sql:"updated_at"`
}

func postgresql() {
	db, err := dbs.Open("postgres", "host=127.0.0.1 port=5432 user=postgres password=postgres dbname=test sslmode=disable", 10, 1)
	if err != nil {
		fmt.Println("连接数据库出错：", err)
		return
	}

	var ndb = dbs.New(db)
	defer ndb.Close()

	//m1(ndb)
	m2(ndb)
}

func m2(db *dbs.DB) {
	fmt.Println("------m2")
	if err := db.PrepareStatement(context.Background(), "get_mail_list", "SELECT id, email, status, created_at, updated_at FROM mail ORDER BY id LIMIT $1"); err != nil {
		fmt.Println("PrepareStatement 发生错误:", err)
		os.Exit(-1)
	}

	mails, err := dbs.Query[[]Mail](context.Background(), db, "get_mail_list", 10)
	if err != nil {
		fmt.Println("查询发生错误:", err)
		os.Exit(-1)
	}
	for _, mail := range mails {
		fmt.Println(mail.Id, mail.Email, mail.Status, mail.CreatedAt, mail.UpdatedAt)
	}
}

func main() {
	postgresql()
}
