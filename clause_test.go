package dbs_test

import (
	"testing"

	"github.com/smartwalle/dbs"
)

func ExpectArgs(args ...interface{}) []interface{} {
	return args
}

func checkClause(t *testing.T, clause dbs.SQLClause, expectSQL string, expectArgs []interface{}) {
	sql, args, err := clause.SQL()
	if err != nil {
		t.Fatal("生成 SQL 语句发生错误:", err)
	}

	if sql != expectSQL {
		t.Fatalf("期望 SQL: %s, 实际 SQL: %s", expectSQL, sql)
	}

	if len(args) != len(expectArgs) {
		t.Fatalf("参数不匹配")
	}

	for index, value := range args {
		if value != expectArgs[index] {
			t.Fatalf("参数 [%d] 不匹配, 期望: %v, 实际: %v", index, expectArgs[index], value)
		}
	}
}

func TestClause_SQL(t *testing.T) {
	var tests = []struct {
		Clause     dbs.SQLClause
		ExpectSQL  string
		ExpectArgs []interface{}
	}{
		{
			Clause:     dbs.SQL("id = ?", 1),
			ExpectSQL:  "id = ?",
			ExpectArgs: ExpectArgs(1),
		},
		{
			Clause:     dbs.SQL("id IN (?)", 1),
			ExpectSQL:  "id IN (?)",
			ExpectArgs: ExpectArgs(1),
		},
		{
			Clause:     dbs.SQL("id IN (?,?)", 1, 2),
			ExpectSQL:  "id IN (?,?)",
			ExpectArgs: ExpectArgs(1, 2),
		},
		{
			Clause:     dbs.SQL("id IN (?)", []interface{}{1, 2, 3}),
			ExpectSQL:  "id IN (?,?,?)",
			ExpectArgs: ExpectArgs(1, 2, 3),
		},
		{
			Clause:     dbs.SQL("enable = ?", dbs.SQL("id = ?", 1)),
			ExpectSQL:  "enable = id = ?",
			ExpectArgs: ExpectArgs(1),
		},
		{
			Clause:     dbs.SQL("enable = (?)", dbs.SQL("id = ?", 2)),
			ExpectSQL:  "enable = (id = ?)",
			ExpectArgs: ExpectArgs(2),
		},
		{
			Clause:     dbs.SQL("enable = (?)", dbs.SQL("id IN (?)", []interface{}{1, 2, 3})),
			ExpectSQL:  "enable = (id IN (?,?,?))",
			ExpectArgs: ExpectArgs(1, 2, 3),
		},
		{
			Clause:     dbs.SQL("sum = sum + ?", 1),
			ExpectSQL:  "sum = sum + ?",
			ExpectArgs: ExpectArgs(1),
		},
		{
			Clause:     dbs.SQL("sum = sum + (?)", dbs.SQL("num + ?", 1)),
			ExpectSQL:  "sum = sum + (num + ?)",
			ExpectArgs: ExpectArgs(1),
		},
		{
			Clause:     dbs.SQL("id = (?)", dbs.SQL("SELECT id FROM user where phone = ?", "12345678901")),
			ExpectSQL:  "id = (SELECT id FROM user where phone = ?)",
			ExpectArgs: ExpectArgs("12345678901"),
		},
		{
			Clause:     dbs.NewSelectBuilder().Table("user").Selects("id,name,age").Where("id = ?", 1),
			ExpectSQL:  "SELECT id,name,age FROM user WHERE id = ?",
			ExpectArgs: ExpectArgs(1),
		},
		{
			Clause:     dbs.NewSelectBuilder().Table("user").Select("id,name,age").Where("id = ?", 2),
			ExpectSQL:  "SELECT id,name,age FROM user WHERE id = ?",
			ExpectArgs: ExpectArgs(2),
		},
		{
			Clause:     dbs.NewSelectBuilder().Table("user").Selects("id").Selects("name").Selects("age").Where("id = ?", 3),
			ExpectSQL:  "SELECT id,name,age FROM user WHERE id = ?",
			ExpectArgs: ExpectArgs(3),
		},
		{
			Clause:     dbs.NewSelectBuilder().Table("user").Select("id").Select("name").Select("age").Where("id = ?", 4),
			ExpectSQL:  "SELECT id,name,age FROM user WHERE id = ?",
			ExpectArgs: ExpectArgs(4),
		},
		{
			Clause:     dbs.NewSelectBuilder().Table("user").Selects("id,name").Select("age").Where("id = ?", 5),
			ExpectSQL:  "SELECT id,name,age FROM user WHERE id = ?",
			ExpectArgs: ExpectArgs(5),
		},
		{
			Clause:     dbs.NewSelectBuilder().Table("user").Selects("id,name,age").Where("id > ?", 2).Limit(10),
			ExpectSQL:  "SELECT id,name,age FROM user WHERE id > ? LIMIT ?",
			ExpectArgs: ExpectArgs(2, int64(10)),
		},
		{
			Clause:     dbs.NewSelectBuilder().Table("user").Selects("id,name,age").Where("id > ?", 2).Limit(10).Offset(11),
			ExpectSQL:  "SELECT id,name,age FROM user WHERE id > ? LIMIT ? OFFSET ?",
			ExpectArgs: ExpectArgs(2, int64(10), int64(11)),
		},
		{
			Clause:     dbs.NewSelectBuilder().Table("user").Selects("id").Select("IF(gender=?,'男','女') sex", 1).Selects("name,age").Where("id = ?", 10),
			ExpectSQL:  "SELECT id,IF(gender=?,'男','女') sex,name,age FROM user WHERE id = ?",
			ExpectArgs: ExpectArgs(1, 10),
		},
		{
			Clause:     dbs.NewSelectBuilder().Table("user").Selects("id,name,age").Where("id IN (?)", []int{1, 2, 3, 4}),
			ExpectSQL:  "SELECT id,name,age FROM user WHERE id IN (?,?,?,?)",
			ExpectArgs: ExpectArgs(1, 2, 3, 4),
		},
		{
			Clause:     dbs.NewSelectBuilder().Table("user").Selects("id,name,age").Where("id IN (?)", []int{1, 2, 3, 4}).Where("type = ?", 1001),
			ExpectSQL:  "SELECT id,name,age FROM user WHERE id IN (?,?,?,?) AND type = ?",
			ExpectArgs: ExpectArgs(1, 2, 3, 4, 1001),
		},
		{
			Clause:     dbs.NewSelectBuilder().Table("user u").Join("LEFT JOIN book b ON b.user_id = u.id").Selects("u.id,u.name,b.name book_name").Where("u.id = ?", 10),
			ExpectSQL:  "SELECT u.id,u.name,b.name book_name FROM user u LEFT JOIN book b ON b.user_id = u.id WHERE u.id = ?",
			ExpectArgs: ExpectArgs(10),
		},
		{
			Clause:     dbs.NewSelectBuilder().Table("user u").Selects("u.id,u.name,u.age").Where("u.id < ?", 100).OrderBy("u.age ASC").OrderBy("u.id ASC,u.updated_time DESC"),
			ExpectSQL:  "SELECT u.id,u.name,u.age FROM user u WHERE u.id < ? ORDER BY u.age ASC,u.id ASC,u.updated_time DESC",
			ExpectArgs: ExpectArgs(100),
		},
		{
			Clause:     dbs.NewSelectBuilder().Table("user u").Selects("u.id,u.name,u.age").Where("u.id < ?", 100).OrderBy("FIELD(u.id, ?)", []int{1, 2, 3, 4, 5, 6}),
			ExpectSQL:  "SELECT u.id,u.name,u.age FROM user u WHERE u.id < ? ORDER BY FIELD(u.id, ?,?,?,?,?,?)",
			ExpectArgs: ExpectArgs(100, 1, 2, 3, 4, 5, 6),
		},
		{
			Clause: dbs.NewSelectBuilder().
				Table("user u").
				Join("LEFT JOIN book b ON b.user_id = u.id").
				Selects("u.id,u.name,b.name book_name").
				Where("u.id < ?", 100).
				Where("b.id IN (?)", []int{31, 32, 33}).
				GroupBy("u.id").GroupBy("u.age").
				OrderBy("FIELD(u.id, ?)", []int{1, 2, 3, 4, 5, 6}).OrderBy("u.age ASC").
				Limit(10).Offset(20),
			ExpectSQL:  "SELECT u.id,u.name,b.name book_name FROM user u LEFT JOIN book b ON b.user_id = u.id WHERE u.id < ? AND b.id IN (?,?,?) GROUP BY u.id,u.age ORDER BY FIELD(u.id, ?,?,?,?,?,?),u.age ASC LIMIT ? OFFSET ?",
			ExpectArgs: ExpectArgs(100, 31, 32, 33, 1, 2, 3, 4, 5, 6, int64(10), int64(20)),
		},
		{
			Clause:     dbs.NewSelectBuilder().Table("user u").Selects("u.id,u.name,u.age").Where("u.id < ?", 100).Limit(10).Offset(10).Count(),
			ExpectSQL:  "SELECT COUNT(1) FROM user u WHERE u.id < ?",
			ExpectArgs: ExpectArgs(100),
		},
		{
			Clause:     dbs.NewInsertBuilder().Table("user").Columns("name,age").Values(1, 2),
			ExpectSQL:  "INSERT INTO user (name,age) VALUES (?,?)",
			ExpectArgs: ExpectArgs(1, 2),
		},
		{
			Clause:     dbs.NewInsertBuilder().Table("user").Columns("name,age").Values(1, 1).Values(2, 2).Values(3, 3),
			ExpectSQL:  "INSERT INTO user (name,age) VALUES (?,?),(?,?),(?,?)",
			ExpectArgs: ExpectArgs(1, 1, 2, 2, 3, 3),
		},
		{
			Clause:     dbs.NewInsertBuilder().Table("user").Columns("name", "age").Values(1, 1).Values(2, 2).Values(3, 3),
			ExpectSQL:  "INSERT INTO user (name,age) VALUES (?,?),(?,?),(?,?)",
			ExpectArgs: ExpectArgs(1, 1, 2, 2, 3, 3),
		},
		{
			Clause:     dbs.NewInsertBuilder().Table("user").Columns("name", "age").Columns("card").Values(1, 1, "111").Values(2, 2, "222").Values(3, 3, "333"),
			ExpectSQL:  "INSERT INTO user (name,age,card) VALUES (?,?,?),(?,?,?),(?,?,?)",
			ExpectArgs: ExpectArgs(1, 1, "111", 2, 2, "222", 3, 3, "333"),
		},
		{
			Clause:     dbs.NewUpdateBuilder().Table("user").Set("name", "n1").Set("age", 10).Where("id = ?", 12),
			ExpectSQL:  "UPDATE user SET name=?,age=? WHERE id = ?",
			ExpectArgs: ExpectArgs("n1", 10, 12),
		},
		{
			Clause:     dbs.NewUpdateBuilder().Table("user").Set("age", dbs.SQL("age+?", 10)).Where("id > ?", 100),
			ExpectSQL:  "UPDATE user SET age=age+? WHERE id > ?",
			ExpectArgs: ExpectArgs(10, 100),
		},
		{
			Clause:     dbs.NewDeleteBuilder().Table("user").Where("id = ?", 100),
			ExpectSQL:  "DELETE FROM user WHERE id = ?",
			ExpectArgs: ExpectArgs(100),
		},
		{
			Clause: dbs.NewSelectBuilder().Table(
				"(? UNION ALL ?) t",
				dbs.NewSelectBuilder().Table("student s").Selects("s.id,s.name").Where("s.id < ?", 100),
				dbs.NewSelectBuilder().Table("teacher t").Selects("t.id,t.name").Where("t.id < ?", 1000),
			).Selects("t.id,t.name"),
			ExpectSQL:  "SELECT t.id,t.name FROM (SELECT s.id,s.name FROM student s WHERE s.id < ? UNION ALL SELECT t.id,t.name FROM teacher t WHERE t.id < ?) t",
			ExpectArgs: ExpectArgs(100, 1000),
		},
	}

	for _, test := range tests {
		checkClause(t, test.Clause, test.ExpectSQL, test.ExpectArgs)
	}
}

func BenchmarkClause_SQL(b *testing.B) {
	for i := 0; i < b.N; i++ {
		var c1 = dbs.SQL("SELECT * FROM user u WHERE u.id = ? AND u.gender = ? ", "1", 2)
		_, _, _ = c1.SQL()
	}
}
