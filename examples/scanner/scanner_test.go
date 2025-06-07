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
	"reflect"
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

func Test_Type(t *testing.T) {
	t.Log("-----Type-----")
	scanIntoType[string](t)
	scanIntoType[string](t)
	scanIntoType[int](t)
	scanIntoType[int8](t)
	scanIntoType[int16](t)
	scanIntoType[int32](t)
	scanIntoType[int64](t)
	scanIntoType[uint](t)
	scanIntoType[uint8](t)
	scanIntoType[uint16](t)
	scanIntoType[uint32](t)
	scanIntoType[uint64](t)
	scanIntoType[float32](t)
	scanIntoType[float64](t)
	scanIntoType[bool](t)
	scanIntoType[Mail](t)
	scanIntoType[*Mail](t)
}

func scanIntoType[T any](t *testing.T) {
	value, err := dbs.Query[T](context.Background(), db, "SELECT id FROM mail WHERE id = 1")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("%+v: %+v \n", reflect.TypeOf(value).Kind(), value)
}

func Test_TypeSlice(t *testing.T) {
	t.Log("-----[]Type-----")
	scanIntoSlice[[]string](t)
	scanIntoSlice[*[]string](t)
	scanIntoSlice[[]int](t)
	scanIntoSlice[*[]int](t)
	scanIntoSlice[[]int8](t)
	scanIntoSlice[*[]int8](t)
	scanIntoSlice[[]int16](t)
	scanIntoSlice[*[]int16](t)
	scanIntoSlice[[]int32](t)
	scanIntoSlice[*[]int32](t)
	scanIntoSlice[[]int64](t)
	scanIntoSlice[*[]int64](t)
	scanIntoSlice[[]uint](t)
	scanIntoSlice[*[]uint](t)
	scanIntoSlice[[]uint8](t)
	scanIntoSlice[*[]uint8](t)
	scanIntoSlice[[]uint16](t)
	scanIntoSlice[*[]uint16](t)
	scanIntoSlice[[]uint32](t)
	scanIntoSlice[*[]uint32](t)
	scanIntoSlice[[]uint64](t)
	scanIntoSlice[*[]uint64](t)
	scanIntoSlice[[]float32](t)
	scanIntoSlice[*[]float32](t)
	scanIntoSlice[[]float64](t)
	scanIntoSlice[*[]float64](t)
	scanIntoSlice[[]Mail](t)
	scanIntoSlice[[]*Mail](t)
	scanIntoSlice[*[]Mail](t)
	scanIntoSlice[*[]*Mail](t)
}

func scanIntoSlice[T any](t *testing.T) {
	value, err := dbs.Query[T](context.Background(), db, "SELECT id FROM mail WHERE id < 5 ORDER BY id ASC")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("[]%+v: %+v \n", reflect.TypeOf(value).Elem().Kind(), value)
}

func Test_Map(t *testing.T) {
	t.Log("-----Map-----")
	scanIntoMap[interface{}](t)
	scanIntoMap[string](t)
	scanIntoMap[int](t)
	scanIntoMap[int8](t)
	scanIntoMap[int16](t)
	scanIntoMap[int32](t)
	scanIntoMap[int64](t)
	scanIntoMap[uint](t)
	scanIntoMap[uint8](t)
	scanIntoMap[uint16](t)
	scanIntoMap[uint32](t)
	scanIntoMap[uint64](t)
	scanIntoMap[float32](t)
	scanIntoMap[float64](t)
	scanIntoMap[bool](t)
}

func scanIntoMap[T any](t *testing.T) {
	mapValue, err := dbs.Query[map[string]T](context.Background(), db, "SELECT id FROM mail WHERE id = 1")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("map[string]%+v: %+v \n", reflect.TypeOf(mapValue).Elem().Kind(), mapValue)
}

func Test_MapSlice(t *testing.T) {
	t.Log("-----MapSlice-----")
	scanIntoMapSlice[interface{}](t)
	scanIntoMapSlice[string](t)
	scanIntoMapSlice[int](t)
	scanIntoMapSlice[int8](t)
	scanIntoMapSlice[int16](t)
	scanIntoMapSlice[int32](t)
	scanIntoMapSlice[int64](t)
	scanIntoMapSlice[uint](t)
	scanIntoMapSlice[uint8](t)
	scanIntoMapSlice[uint16](t)
	scanIntoMapSlice[uint32](t)
	scanIntoMapSlice[uint64](t)
	scanIntoMapSlice[float32](t)
	scanIntoMapSlice[float64](t)
}

func scanIntoMapSlice[T any](t *testing.T) {
	mapValue, err := dbs.Query[[]map[string]T](context.Background(), db, "SELECT id FROM mail WHERE id < 5 ORDER BY id ASC")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("[]map[string]%+v: %+v \n", reflect.TypeOf(mapValue).Elem().Elem().Kind(), mapValue)
}
