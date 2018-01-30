package dbs

import (
	"testing"
	"fmt"
)

func TestNewStatement(t *testing.T) {
	var st = NewStatement("a=", NewStatement("SELECT tt.id, tt.name FROM test_table AS tt WHERE tt.id=?", 100))
	fmt.Println(st.ToSQL())
}

func TestNewStatements(t *testing.T) {
	var sts statements
	sts = append(sts, Alias("a", "c1"), Alias("b", "c2"), Alias("c", "c3"))
	fmt.Println(sts.ToSQL())
}

func TestAlias(t *testing.T) {
	var a = Alias(NewStatement("SELECT tt.id, tt.name FROM test_table AS tt WHERE tt.id=?", 100), "a")
	fmt.Println(a.ToSQL())
}

func TestCase(t *testing.T) {
	var c = Case("a").When("1", "'男'").When("2", "'女'")
	fmt.Println(c.ToSQL())

	var c2 = Alias(c, "cc")
	fmt.Println(c2.ToSQL())
}