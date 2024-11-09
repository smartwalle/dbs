package main

import (
	"context"
	_ "github.com/lib/pq"
	"github.com/smartwalle/dbs"
	"log"
	"os"
	"testing"
	"time"
)

type Mail struct {
	Id        int32      `sql:"id"`
	Email     string     `sql:"email"`
	Status    string     `sql:"status"`
	CreatedAt *time.Time `sql:"created_at"`
	UpdatedAt time.Time  `sql:"updated_at"`
}

var db dbs.Database

func TestMain(m *testing.M) {
	var err error
	db, err = dbs.Open("postgres", "host=127.0.0.1 port=5432 user=postgres password=postgres dbname=test sslmode=disable", 10, 1)
	if err != nil {
		log.Println("连接数据库出错：", err)
		return
	}
	var code = m.Run()
	db.Close()
	os.Exit(code)
}

func Test_PointerSlice(t *testing.T) {
	t.Log("-----[]*Mail-----")

	mails, err := dbs.Query[[]*Mail](context.Background(), db, "SELECT * FROM mail")
	if err != nil {
		log.Println(err)
	}
	for _, mail := range mails {
		t.Logf("指针切片: %+v \n", mail)
	}
}

func Test_StructSlice(t *testing.T) {
	t.Log("-----[]Mial-----")

	mails, err := dbs.Query[[]Mail](context.Background(), db, "SELECT * FROM mail")
	if err != nil {
		log.Println(err)
	}
	for _, mail := range mails {
		t.Logf("结构体切片: %+v \n", mail)
	}
}

func Test_Pointer(t *testing.T) {
	t.Log("-----*Mail-----")
	mail, err := dbs.Query[*Mail](context.Background(), db, "SELECT * FROM mail WHERE email = '1@qq.com'")
	if err != nil {
		log.Println(err)
	}
	t.Logf("指针: %+v \n", mail)
}

func Test_Struct(t *testing.T) {
	t.Log("-----Mail-----")
	mail, err := dbs.Query[Mail](context.Background(), db, "SELECT * FROM mail WHERE email = '1@qq.com'")
	if err != nil {
		log.Println(err)
	}
	t.Logf("结构体: %+v \n", mail)
}

func Test_int64(t *testing.T) {
	t.Log("-----int64-----")
	id, err := dbs.Query[int64](context.Background(), db, "SELECT id FROM mail WHERE email = 'qq@qq.com'")
	if err != nil {
		log.Println(err)
	}
	t.Logf("int64: %+v \n", id)
}

func Test_int64Slice(t *testing.T) {
	t.Log("-----int64 slice-----")
	ids, err := dbs.Query[[]int64](context.Background(), db, "SELECT id FROM mail")
	if err != nil {
		log.Println(err)
	}
	t.Logf("int64 slice: %+v \n", ids)
}

func Test_string(t *testing.T) {
	t.Log("-----string-----")
	email, err := dbs.Query[string](context.Background(), db, "SELECT email FROM mail WHERE email = 'qq@qq.com'")
	if err != nil {
		log.Println(err)
	}
	t.Logf("string: %+v \n", email)
}

func Test_stringSlice(t *testing.T) {
	t.Log("-----string slice-----")
	emails, err := dbs.Query[[]string](context.Background(), db, "SELECT email FROM mail")
	if err != nil {
		log.Println(err)
	}
	t.Logf("string: %+v \n", emails)
}
