package dba

import (
	"testing"
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"fmt"
	"time"
	"os"
)

type Human struct {
	Id       int       `sql:"id"`
	Name     string    `sql:"name"`
	Gender   int       `sql:"gender"`
	Birthday time.Time `sql:"birthday"`
	Type     Type
}

type Type struct {
	Id   int    `sql:"tid"`
	Name string `sql:"tname"`
}

var db *sql.DB

func getDB() *sql.DB {
	if db == nil {
		var ndb, err = sql.Open("mysql", "root:smok2015@tcp(192.168.192.250:3306)/test?parseTime=true")
		if err != nil {
			fmt.Println("连接数据库出错:", err)
			os.Exit(-1)
		}
		db = ndb
	}
	return db
}

func TestBind(t *testing.T) {
	var db = getDB()

	//stmt, err := db.Prepare()
	//if err != nil {
	//	fmt.Println("Prepare", err)
	//	return
	//}
	//defer stmt.Close()

	//rows, err := stmt.Query()
	//if err != nil {
	//	fmt.Println("Query", err)
	//	return
	//}

	var rows, err = Query(db, "SELECT h.id, h.name, h.gender, h.birthday, t.id as tid, t.name as tname from human as h LEFT JOIN h_type as t ON h.type=t.id")
	if err != nil {
		return
	}

	var hList []*Human
	err = Bind(rows, &hList)
	if err != nil {
		fmt.Println("Bind", err)
		return
	}

	for _, h := range hList {
		fmt.Println(h.Id, h.Name, h.Gender, h.Birthday, h.Type.Name, h.Type.Id)
	}
}


