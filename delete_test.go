package dbs

import (
	"fmt"
	"testing"
)

func TestDeleteBuilder_ToSQL(t *testing.T) {
	var db = NewDeleteBuilder()
	db.Alias("u", "b")
	db.Table("user", "AS u")
	db.Where(SQL("u.id=?", 10))
	db.Limit(1)
	db.Offset(2)
	fmt.Println(db.ToSQL())
}
