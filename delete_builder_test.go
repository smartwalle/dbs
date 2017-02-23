package dba

import (
	"testing"
	"fmt"
)

func TestDeleteBuilder_ToSQL(t *testing.T) {
	var db = NewDeleteBuilder()
	db.From("t1", "t2")
	db.Join("INNER JOIN", "t2")
	db.Where("t1.id=?", "test")
	db.Limit(1)

	fmt.Println(db.ToSQL())
}