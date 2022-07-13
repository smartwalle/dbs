package dbs

import (
	"fmt"
	"testing"
)

func TestNewStatement(t *testing.T) {
	fmt.Println("===== Statement =====")
	var st = NewStatement("a=", NewStatement("SELECT tt.id, tt.name FROM test_table AS tt WHERE tt.id=?", 100))
	fmt.Println(st.SQL())
}

func TestNewStatements(t *testing.T) {
	fmt.Println("===== Statements =====")
	var sts statements
	sts = append(sts, Alias("a", "c1"), Alias("b", "c2"), Alias("c", "c3"))
	fmt.Println(sts.SQL())
}

func TestAlias(t *testing.T) {
	fmt.Println("===== Alias =====")
	var a = Alias(NewStatement("SELECT tt.id, tt.name FROM test_table AS tt WHERE tt.id=?", 100), "a")
	fmt.Println(a.SQL())
}

func TestCase(t *testing.T) {
	fmt.Println("===== Case =====")
	var c = Case().When("1", "'男'").When("2", "'女'").Else("33")
	fmt.Println(c.SQL())

	var c2 = Alias(c, "cc")
	fmt.Println(c2.SQL())
}

func TestAND(t *testing.T) {
	fmt.Println("===== AND =====")
	var a1 = AND(AND(SQL("a=?", 10), SQL("b=?", 20)), AND(SQL("c=?", 30), SQL("d=?", 40)))
	a1.Append("e=?", 50)
	a1.Append(AND(SQL("f=?", 60)))
	fmt.Println(a1.SQL())

	var a2 = AND(OR(SQL("a=?", 10), SQL("b=?", 20)))
	fmt.Println(a2.SQL())
}

func TestOR(t *testing.T) {
	fmt.Println("===== OR =====")
	var a1 = AND(OR(SQL("a=?", 10), SQL("b=?", 20)), OR(SQL("c=?", 30), SQL("d=?", 40)))
	fmt.Println(a1.SQL())
}

func TestSetStmt(t *testing.T) {
	fmt.Println("===== SET =====")
	var s1 setStmts

	s1 = append(s1, newSet("c", 100))
	s1 = append(s1, newSet("b", 200))
	s1 = append(s1, newSet("c", SQL("b+?", 200)))

	fmt.Println(s1.SQL())
}

func TestIN(t *testing.T) {
	fmt.Println("===== IN =====")
	var i1 = IN("a", []int{1, 2, 3, 4})
	fmt.Println(i1.SQL())

	var i2 = IN("b", []int{})
	fmt.Println(i2.SQL())

	var a []int = nil
	var i3 = IN("b", a)
	fmt.Println(i3.SQL())

	var sb = NewSelectBuilder()
	sb.Selects("u.id", "u.name")
	sb.From("user", "AS u")
	sb.Where("u.id = ?", 10)

	var i4 = IN("b", sb)
	fmt.Println(i4.SQL())
}

func TestEq(t *testing.T) {
	fmt.Println("===== EQ =====")
	var e = Eq{"a": 10, "b": NewStatement("SELECT tt.id, tt.name FROM test_table AS tt WHERE tt.id=?", 1200), "c": nil}
	fmt.Println(e.SQL())
}

func TestLike(t *testing.T) {
	fmt.Println("===== Like =====")
	var l = Like("a", "%", "haha", "%")
	fmt.Println(l.SQL())

	var nl = NotLike("a", "%", "hehe", "%")
	fmt.Println(nl.SQL())
}
