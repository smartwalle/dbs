package dbs_test

import (
	"testing"

	"github.com/smartwalle/dbs"
)

func TestExplain(t *testing.T) {
	type AliasInt int

	var tests = []struct {
		Clause    dbs.SQLClause
		ExpectSQL string
	}{
		{
			Clause:    dbs.SQL("id = ?", 10),
			ExpectSQL: "id = 10",
		},
		{
			Clause:    dbs.SQL("id = ?", AliasInt(10)),
			ExpectSQL: "id = 10",
		},
		{
			Clause:    dbs.SQL("id IN (?)", []int{1, 2, 3, 4, 5}),
			ExpectSQL: "id IN (1,2,3,4,5)",
		},
		{
			Clause:    dbs.SQL("id IN (?)", []AliasInt{1, 2, 3, 4, 5}),
			ExpectSQL: "id IN (1,2,3,4,5)",
		},
		{
			Clause:    dbs.SQL("id = (?)", dbs.SQL("SELECT id FROM user where phone = ?", "12345678901")),
			ExpectSQL: "id = (SELECT id FROM user where phone = '12345678901')",
		},
	}

	for _, test := range tests {
		var sql, err = dbs.Explain(test.Clause)
		if err != nil {
			t.Fatal("生成 SQL 语句发生错误:", err)
		}
		if sql != test.ExpectSQL {
			t.Fatalf("期望 SQL: %s, 实际 SQL: %s", test.ExpectSQL, sql)
		}
	}
}
