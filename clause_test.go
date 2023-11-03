package dbs_test

import (
	"github.com/smartwalle/dbs"
	"testing"
)

func TestNewClause(t *testing.T) {
	var clause = dbs.NewClause("a=", dbs.NewClause("SELECT tt.id, tt.name FROM test_table AS tt WHERE tt.id=?", 100))

	check(t, clause, "a=(SELECT tt.id, tt.name FROM test_table AS tt WHERE tt.id=?)", []interface{}{100})
}

func TestAlias(t *testing.T) {
	var a = dbs.Alias(dbs.NewClause("SELECT tt.id, tt.name FROM test_table AS tt WHERE tt.id=?", 100), "a")

	check(t, a, "(SELECT tt.id, tt.name FROM test_table AS tt WHERE tt.id=?) AS a", []interface{}{100})
}

func TestCase(t *testing.T) {
	var c1 = dbs.Case().When("1", "'男'").When("2", "'女'").Else("33")

	check(t, c1, "CASE WHEN 1 THEN '男' WHEN 2 THEN '女' ELSE 33 END", []interface{}{})
	check(t, dbs.Alias(c1, "cc"), "(CASE WHEN 1 THEN '男' WHEN 2 THEN '女' ELSE 33 END) AS cc", []interface{}{})
}

func TestAND(t *testing.T) {
	var and = dbs.AND(dbs.AND(dbs.SQL("a=?", 10), dbs.SQL("b=?", 20)), dbs.AND(dbs.SQL("c=?", 30), dbs.SQL("f=?", 40)))
	and.Append("e=?", 50)
	and.Append(dbs.AND(dbs.SQL("f=?", 60)))

	check(t, and, "(a=? AND b=?) AND (c=? AND f=?) AND e=? AND (f=?)", []interface{}{10, 20, 30, 40, 50, 60})
}

func TestOR(t *testing.T) {
	check(t, dbs.OR(dbs.SQL("a=?", 10), dbs.SQL("b=?", 20)), "a=? OR b=?", []interface{}{10, 20})
	check(t, dbs.AND(dbs.OR(dbs.SQL("a=?", 10), dbs.SQL("b=?", 20)), dbs.OR(dbs.SQL("c=?", 30), dbs.SQL("f=?", 40))), "(a=? OR b=?) AND (c=? OR f=?)", []interface{}{10, 20, 30, 40})
}

func TestIN(t *testing.T) {
	check(t, dbs.IN("a", []int{1, 2, 3, 4}), "a IN (?, ?, ?, ?)", []interface{}{1, 2, 3, 4})
	check(t, dbs.IN("b", []int{}), "b IN ()", []interface{}{})
	check(t, dbs.IN("b", nil), "b IN ()", []interface{}{})
	check(t, dbs.IN("b", dbs.Select("u.id", "u.name").From("user", "AS u").Where("u.id = ?", 10)), "b IN (SELECT u.id, u.name FROM user AS u WHERE u.id = ?)", []interface{}{10})
}

func TestEq(t *testing.T) {
	check(t, dbs.Eq{"a": 10}, "(a = ?)", []interface{}{10})
	check(t, dbs.Eq{"b": dbs.NewClause("SELECT tt.id, tt.name FROM test_table AS tt WHERE tt.id=?", 1200)}, "(b = SELECT tt.id, tt.name FROM test_table AS tt WHERE tt.id=?)", []interface{}{1200})
	check(t, dbs.Eq{"c": nil}, "(c IS NULL)", nil)
}

func TestLike(t *testing.T) {
	check(t, dbs.Like("a", "%", "haha", "%"), "a LIKE ?", []interface{}{"%haha%"})
	check(t, dbs.Like("a", "%", "haha"), "a LIKE ?", []interface{}{"%haha"})
	check(t, dbs.Like("a", "haha", "%"), "a LIKE ?", []interface{}{"haha%"})
	check(t, dbs.NotLike("a", "%", "hehe", "%"), "a NOT LIKE ?", []interface{}{"%hehe%"})
	check(t, dbs.NotLike("a", "%", "hehe", ""), "a NOT LIKE ?", []interface{}{"%hehe"})
	check(t, dbs.NotLike("a", "hehe", ""), "a NOT LIKE ?", []interface{}{"hehe"})
}

func check(t *testing.T, clause testClause, expectClause string, expectArgs []interface{}) {
	sql, args, _ := clause.SQL()

	if sql != expectClause {
		t.Fatalf("期望 SQL: %s, 实际 SQL: %s", expectClause, sql)
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

type testClause interface {
	SQL() (string, []interface{}, error)
}
