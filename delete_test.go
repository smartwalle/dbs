package dbs

import (
	"fmt"
	"testing"
)

func TestDeleteBuilder(t *testing.T) {
	fmt.Println("===== DeleteBuilder =====")
	var db = NewDeleteBuilder()
	db.Alias("u", "b")
	db.Table("user", "AS u")
	db.Where("u.id=?", 10)
	db.Limit(1)
	db.Offset(2)
	fmt.Println(db.ToSQL())
}
