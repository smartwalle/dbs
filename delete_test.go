package dbs

import (
	"testing"
	"fmt"
)

func TestDeleteBuilder_ToSQL(t *testing.T) {
	var db = NewDeleteBuilder()
	db.Alias("u", "b")
	db.Table("user", "AS u")
	db.Where("u.id=?", 10)
	db.Limit(1)
	fmt.Println(db.ToSQL())
}