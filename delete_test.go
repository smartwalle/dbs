package dba

import (
	"testing"
	"fmt"
)

func TestDeleteBuilder_ToSQL(t *testing.T) {
	var db = NewDeleteBuilder()
	db.Table("user")
	db.Where("id=?", 10)
	db.Limit(1)
	fmt.Println(db.ToSQL())
}