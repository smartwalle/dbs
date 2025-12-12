package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	_ "github.com/lib/pq"
	"github.com/smartwalle/dbs"
	"github.com/smartwalle/dbs/postgres"
	"log"
	"os"
	"reflect"
	"testing"
	"time"
)

type Base struct {
	Id int32 `sql:"id;auto_increment"`
}

type Mail struct {
	Base
	Email     string     `sql:"email"`
	Status    string     `sql:"status"`
	CreatedAt *time.Time `sql:"created_at"`
	UpdatedAt time.Time  `sql:"updated_at"`
	Extra     *Extra     `sql:"extra"`
}

func (Mail) TableName() string {
	return "mail"
}

func (Mail) PrimaryKey() string {
	return "id"
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
	db = NewPgx()

	var code = m.Run()
	db.Close()
	os.Exit(code)
}

func NewPostgres() dbs.Database {
	rawDB, err := sql.Open("postgres", "host=127.0.0.1 port=5432 user=postgres password=postgres dbname=test sslmode=disable")
	if err != nil {
		log.Println("连接数据库出错：", err)
		os.Exit(-1)
	}
	return dbs.New(rawDB)
}

func NewPgx() dbs.Database {
	var config, err = pgx.ParseConfig("host=127.0.0.1 port=5432 user=postgres password=postgres dbname=test sslmode=disable")
	if err != nil {
		log.Println("连接数据库出错：", err)
		os.Exit(-1)
	}
	rawDB := stdlib.OpenDB(*config)
	return dbs.New(rawDB)
}

func Test_Encode(t *testing.T) {
	var repo = dbs.NewRepository[Mail](db)
	repo.UseDialect(postgres.Dialect())

	var xxx, er = repo.Select(context.Background(), 100091)
	t.Log(xxx, er)

	t.Log(repo.Delete(context.Background(), 10025))

	t.Log(repo.Update(context.Background(), 10024, map[string]interface{}{"status": "111"}))

	//var mail = &Mail{}
	//mail.Email = "qq@qq.com"
	//mail.UpdatedAt = time.Now()
	//mail.CreatedAt = &mail.UpdatedAt
	//_, err := repo.Insert(context.Background(), mail)
	//if err != nil {
	//	t.Fatal(err)
	//}
}

func Test_Type(t *testing.T) {
	t.Log("-----Type-----")
	scanIntoType[string](t, true)
	scanIntoType[string](t, true)
	scanIntoType[int](t, true)
	scanIntoType[int8](t, true)
	scanIntoType[int16](t, true)
	scanIntoType[int32](t, true)
	scanIntoType[int64](t, true)
	scanIntoType[uint](t, true)
	scanIntoType[uint8](t, true)
	scanIntoType[uint16](t, true)
	scanIntoType[uint32](t, true)
	scanIntoType[uint64](t, true)
	scanIntoType[float32](t, true)
	scanIntoType[float64](t, true)
	scanIntoType[bool](t, true)
	scanIntoType[Mail](t, true)
	scanIntoType[*Mail](t, true)
}

func scanIntoType[T any](t Tester, enableLog bool) {
	value, err := dbs.Query[T](context.Background(), db, "SELECT id FROM mail WHERE id = 1")
	if err != nil {
		t.Fatal(err)
	}
	if enableLog {
		t.Logf("%+v: %+v \n", reflect.TypeOf(value).Kind(), value)
	}
}

func Test_TypeSlice(t *testing.T) {
	t.Log("-----[]Type-----")
	scanIntoSlice[[]string](t, true)
	scanIntoSlice[*[]string](t, true)
	scanIntoSlice[[]int](t, true)
	scanIntoSlice[*[]int](t, true)
	scanIntoSlice[[]int8](t, true)
	scanIntoSlice[*[]int8](t, true)
	scanIntoSlice[[]int16](t, true)
	scanIntoSlice[*[]int16](t, true)
	scanIntoSlice[[]int32](t, true)
	scanIntoSlice[*[]int32](t, true)
	scanIntoSlice[[]int64](t, true)
	scanIntoSlice[*[]int64](t, true)
	scanIntoSlice[[]uint](t, true)
	scanIntoSlice[*[]uint](t, true)
	scanIntoSlice[[]uint8](t, true)
	scanIntoSlice[*[]uint8](t, true)
	scanIntoSlice[[]uint16](t, true)
	scanIntoSlice[*[]uint16](t, true)
	scanIntoSlice[[]uint32](t, true)
	scanIntoSlice[*[]uint32](t, true)
	scanIntoSlice[[]uint64](t, true)
	scanIntoSlice[*[]uint64](t, true)
	scanIntoSlice[[]float32](t, true)
	scanIntoSlice[*[]float32](t, true)
	scanIntoSlice[[]float64](t, true)
	scanIntoSlice[*[]float64](t, true)
	scanIntoSlice[[]Mail](t, true)
	scanIntoSlice[[]*Mail](t, true)
	scanIntoSlice[*[]Mail](t, true)
	scanIntoSlice[*[]*Mail](t, true)
}

func scanIntoSlice[T any](t Tester, enableLog bool) {
	value, err := dbs.Query[T](context.Background(), db, "SELECT id FROM mail WHERE id < 5 ORDER BY id ASC")
	if err != nil {
		t.Fatal(err)
	}
	if enableLog {
		t.Logf("[]%+v: %+v \n", reflect.TypeOf(value).Elem().Kind(), value)
	}
}

func Test_Map(t *testing.T) {
	t.Log("-----Map-----")
	scanIntoMap[interface{}](t, true)
	scanIntoMap[string](t, true)
	scanIntoMap[int](t, true)
	scanIntoMap[int8](t, true)
	scanIntoMap[int16](t, true)
	scanIntoMap[int32](t, true)
	scanIntoMap[int64](t, true)
	scanIntoMap[uint](t, true)
	scanIntoMap[uint8](t, true)
	scanIntoMap[uint16](t, true)
	scanIntoMap[uint32](t, true)
	scanIntoMap[uint64](t, true)
	scanIntoMap[float32](t, true)
	scanIntoMap[float64](t, true)
	scanIntoMap[bool](t, true)
}

func scanIntoMap[T any](t Tester, enableLog bool) {
	mapValue, err := dbs.Query[map[string]T](context.Background(), db, "SELECT id FROM mail WHERE id = 1")
	if err != nil {
		t.Fatal(err)
	}
	if enableLog {
		t.Logf("map[string]%+v: %+v \n", reflect.TypeOf(mapValue).Elem().Kind(), mapValue)
	}
}

func Test_MapSlice(t *testing.T) {
	t.Log("-----[]Map-----")
	scanIntoMapSlice[interface{}](t, true)
	scanIntoMapSlice[string](t, true)
	scanIntoMapSlice[int](t, true)
	scanIntoMapSlice[int8](t, true)
	scanIntoMapSlice[int16](t, true)
	scanIntoMapSlice[int32](t, true)
	scanIntoMapSlice[int64](t, true)
	scanIntoMapSlice[uint](t, true)
	scanIntoMapSlice[uint8](t, true)
	scanIntoMapSlice[uint16](t, true)
	scanIntoMapSlice[uint32](t, true)
	scanIntoMapSlice[uint64](t, true)
	scanIntoMapSlice[float32](t, true)
	scanIntoMapSlice[float64](t, true)
}

func scanIntoMapSlice[T any](t Tester, enableLog bool) {
	mapValue, err := dbs.Query[[]map[string]T](context.Background(), db, "SELECT id FROM mail WHERE id < 5 ORDER BY id ASC")
	if err != nil {
		t.Fatal(err)
	}
	if enableLog {
		t.Logf("[]map[string]%+v: %+v \n", reflect.TypeOf(mapValue).Elem().Elem().Kind(), mapValue)
	}
}

type Tester interface {
	Fatal(args ...any)
	Logf(format string, args ...any)
}

func Benchmark_Type(b *testing.B) {
	for i := 0; i < b.N; i++ {
		scanIntoType[int64](b, false)
	}
}

func Benchmark_TypeSlice(b *testing.B) {
	for i := 0; i < b.N; i++ {
		scanIntoSlice[[]int64](b, false)
	}
}

func Benchmark_Map(b *testing.B) {
	for i := 0; i < b.N; i++ {
		scanIntoMap[int64](b, false)
	}
}

func Benchmark_MapSlice(b *testing.B) {
	for i := 0; i < b.N; i++ {
		scanIntoMapSlice[interface{}](b, false)
	}
}
