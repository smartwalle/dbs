package dba

import (
	"testing"
	"github.com/smartwalle/going/sql"
	_ "github.com/go-sql-driver/mysql"
	"fmt"
)

func TestTx_Begin(t *testing.T) {
	var m = sql.NewSQL("mysql", "root:smok2015@tcp(192.168.192.250:3306)/titan?parseTime=true", 10, 5)
	var tx = NewTx(m.GetSession().DB)

	var userList []*User
	tx.Append("SELECT id, username FROM user", nil, &userList)
	fmt.Println(tx.Commit())

	for _, u := range userList {
		fmt.Println(u.Id, u.Username)
	}
}

type User struct {
	Id        int64      `json:"id"            sql:"id"`
	Username  string     `json:"username"      sql:"username"`
}