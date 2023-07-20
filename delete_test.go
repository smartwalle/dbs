package dbs_test

import (
	"github.com/smartwalle/dbs"
	"testing"
)

func TestDeleteBuilder(t *testing.T) {
	var db = dbs.NewDeleteBuilder()
	db.Alias("u")
	db.Table("user", "AS u")
	db.Where("u.id=?", 10)
	db.Limit(1)
	db.Offset(2)

	check(t, db, "DELETE u FROM user AS u WHERE u.id=? LIMIT ? OFFSET ?", []interface{}{10, int64(1), int64(2)})
}
