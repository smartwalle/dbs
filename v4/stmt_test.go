package dbs

import (
	"testing"
	"fmt"
)

func TestNewStatement(t *testing.T) {
	fmt.Println("===== Statement =====")
	var st = NewStatement("a=", NewStatement("SELECT tt.id, tt.name FROM test_table AS tt WHERE tt.id=?", 100))
	fmt.Println(st.ToSQL())
}

func TestNewStatements(t *testing.T) {
	fmt.Println("===== Statements =====")
	var sts statements
	sts = append(sts, Alias("a", "c1"), Alias("b", "c2"), Alias("c", "c3"))
	fmt.Println(sts.ToSQL())
}

func TestAlias(t *testing.T) {
	fmt.Println("===== Alias =====")
	var a = Alias(NewStatement("SELECT tt.id, tt.name FROM test_table AS tt WHERE tt.id=?", 100), "a")
	fmt.Println(a.ToSQL())
}

func TestCase(t *testing.T) {
	fmt.Println("===== Case =====")
	var c = Case("a").When("1", "'男'").When("2", "'女'")
	fmt.Println(c.ToSQL())

	var c2 = Alias(c, "cc")
	fmt.Println(c2.ToSQL())
}

func TestAND(t *testing.T) {
	fmt.Println("===== AND =====")
	var a1 = AND(AND(SQL("a=?", 10), SQL("b=?", 20)), AND(SQL("c=?", 30), SQL("d=?", 40)))
	a1.Append("e=?", 50)
	a1.Append(AND(SQL("f=?", 60)))
	fmt.Println(a1.ToSQL())

	var a2 = AND(OR(SQL("a=?", 10), SQL("b=?", 20)))
	fmt.Println(a2.ToSQL())
}

func TestOR(t *testing.T) {
	fmt.Println("===== OR =====")
	var a1 = AND(OR(SQL("a=?", 10), SQL("b=?", 20)), OR(SQL("c=?", 30), SQL("d=?", 40)))
	fmt.Println(a1.ToSQL())
}