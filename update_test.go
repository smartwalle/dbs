package dba

import (
	"testing"
	"fmt"
)

func TestUpdate(t *testing.T) {
	var db = getDB()
	var _, err = Update(db, "human", map[string]interface{}{"name": "testname"}, NewWhere("id=?", 1))
	fmt.Println("更新操作:", err)
}
