package main

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	_ "github.com/lib/pq"
	"github.com/smartwalle/dbs"
	"log"
	"os"
	"testing"
	"time"
)

type Base struct {
	Id int32 `sql:"id"`
}

type Mail struct {
	Base
	Email     string     `sql:"email"`
	Status    string     `sql:"status"`
	CreatedAt *time.Time `sql:"created_at"`
	UpdatedAt time.Time  `sql:"updated_at"`
	Extra     Extra      `sql:"extra"`
}

type Extra struct {
	Age  int    `json:"age"`
	City string `json:"city"`
	Name string `json:"name"`
}

func (a Extra) Value() (driver.Value, error) {
	return json.Marshal(a)
}

func (a *Extra) Scan(value interface{}) error {
	b, ok := value.([]byte)
	if !ok {
		return errors.New("type assertion to []byte failed")
	}
	return json.Unmarshal(b, &a)
}

var db dbs.Database

func TestMain(m *testing.M) {
	var err error
	db, err = dbs.Open("postgres", "host=127.0.0.1 port=5432 user=postgres password=postgres dbname=test sslmode=disable", 1, 1)
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
		t.Fatal(err)
	}
	for _, mail := range mails {
		t.Logf("指针切片: %+v \n", mail)
	}
}

func Test_PointerSlicePointer(t *testing.T) {
	t.Log("-----*[]*Mail-----")

	mails, err := dbs.Query[*[]*Mail](context.Background(), db, "SELECT * FROM mail")
	if err != nil {
		t.Fatal(err)
	}
	for _, mail := range *mails {
		t.Logf("指针切片指针: %+v \n", mail)
	}
}

func Test_StructSlice(t *testing.T) {
	t.Log("-----[]Mial-----")

	mails, err := dbs.Query[[]Mail](context.Background(), db, "SELECT * FROM mail")
	if err != nil {
		t.Fatal(err)
	}
	for _, mail := range mails {
		t.Logf("结构体切片: %+v \n", mail)
	}
}

func Test_StructSlicePointer(t *testing.T) {
	t.Log("-----*[]Mial-----")

	mails, err := dbs.Query[*[]Mail](context.Background(), db, "SELECT * FROM mail")
	if err != nil {
		t.Fatal(err)
	}
	for _, mail := range *mails {
		t.Logf("结构体切片指针: %+v \n", mail)
	}
}

func Test_Pointer(t *testing.T) {
	t.Log("-----*Mail-----")
	mail, err := dbs.Query[*Mail](context.Background(), db, "SELECT * FROM mail WHERE email = $1", "1@qq.com")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("指针: %+v \n", mail)
}

func Test_Struct(t *testing.T) {
	t.Log("-----Mail-----")
	mail, err := dbs.Query[Mail](context.Background(), db, "SELECT * FROM mail WHERE email = $1", "1@qq.com")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("结构体: %+v \n", mail)
}

func Test_Int64(t *testing.T) {
	t.Log("-----int64-----")
	id, err := dbs.Query[int64](context.Background(), db, "SELECT id FROM mail WHERE email = $1", "qq@qq.com")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("int64: %+v \n", id)
}

func Test_Int64Pointer(t *testing.T) {
	t.Log("-----*int64-----")
	id, err := dbs.Query[*int64](context.Background(), db, "SELECT id FROM mail WHERE email = $1", "qq@qq.com")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("int64: %+v \n", *id)
}

func Test_Int64Slice(t *testing.T) {
	t.Log("-----[]int64-----")
	ids, err := dbs.Query[[]int64](context.Background(), db, "SELECT id FROM mail")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("int64 slice: %+v \n", ids)
}

func Test_Int64SlicePointer(t *testing.T) {
	t.Log("-----*[]int64-----")
	ids, err := dbs.Query[*[]int64](context.Background(), db, "SELECT id FROM mail")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("int64 slice: %+v \n", ids)
}

func Test_String(t *testing.T) {
	t.Log("-----string-----")
	email, err := dbs.Query[string](context.Background(), db, "SELECT email FROM mail WHERE email = $1", "qq@qq.com")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("string: %+v \n", email)
}

func Test_StringPointer(t *testing.T) {
	t.Log("-----*string-----")
	email, err := dbs.Query[*string](context.Background(), db, "SELECT email FROM mail WHERE email = $1", "qq@qq.com")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("string: %+v \n", *email)
}

func Test_StringSlice(t *testing.T) {
	t.Log("-----[]string-----")
	emails, err := dbs.Query[[]string](context.Background(), db, "SELECT status FROM mail")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("string: %+v \n", emails)
}

func Test_StringSlicePointer(t *testing.T) {
	t.Log("-----*[]string-----")
	emails, err := dbs.Query[*[]string](context.Background(), db, "SELECT status FROM mail")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("string: %+v \n", emails)
}
