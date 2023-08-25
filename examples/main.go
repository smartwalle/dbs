package main

import (
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	"github.com/smartwalle/dbs"
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
	db, err := dbs.Open("postgres", "host=192.168.1.99 port=5432 user=postgres password=postgres dbname=test sslmode=disable", 10, 1)
	if err != nil {
		fmt.Println("连接数据库出错：", err)
		return
	}

	var ndb = dbs.New(db)
	defer ndb.Close()

	var sb = dbs.NewSelectBuilder()
	sb.UsePlaceholder(dbs.DollarPlaceholder)
	sb.Selects("id", "email", "status", "created_at", "updated_at")
	sb.From("mail")
	sb.Limit(10)
	sb.OrderBy("id")

	var mails []*Mail
	if err = sb.Scan(ndb, &mails); err != nil {
		fmt.Println("查询发生错误:", err)
		return
	}

	for _, mail := range mails {
		fmt.Println(mail.Id, mail.Email, mail.Status, mail.CreatedAt, mail.UpdatedAt)
	}
}

func main() {
	postgresql()
}
