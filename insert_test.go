package dba

import (
	"testing"
	"fmt"
	"time"
)

func TestInsert(t *testing.T) {
	var db = getDB()
	var _, err = Insert(db, "human", map[string]interface{}{"name": "new name", "birthday": time.Now()})
	fmt.Println("添加操作:", err)
}
