package dbs

import (
	"fmt"
	"testing"
)

func TestSQL(t *testing.T) {
	var c1 = SQL("a=?", 1)
	c1.Append("AND b=?", 11)
	fmt.Println(c1.ToSQL())
}

func TestWhere_AND(t *testing.T) {
	var wa1 = AND()
	wa1.Append(SQL("c1=?", 10), SQL("c2=?", 20), OR(SQL("c1=?", 11), SQL("c2=?", 21)), AND(SQL("c3=?", 30)))
	fmt.Println(wa1.ToSQL())

	var wa2 = AND()
	wa2.Append(SQL("c1=?", 10), SQL("c2=?", 10))
	fmt.Println(wa2.ToSQL())
}

func TestWhere_OR(t *testing.T) {
	var wo1 = OR()
	wo1.Append(SQL("c1=?", 10), SQL("c2=?", 20), AND(SQL("c3=?", 30), SQL("c4=?", 40)))
	fmt.Println(wo1.ToSQL())
}

func TestWhere_IN(t *testing.T) {
	var wi1 = IN("c1", []int{1, 2, 3, 4, 5})
	fmt.Println(wi1.ToSQL())

	var wi2 = AND(IN("c1", []int{1, 2, 3, 4, 5}), SQL("c2=?", 10))
	fmt.Println(wi2.ToSQL())
}

func TestStatement(t *testing.T) {
	var s = SQL("a=?", 10)
	s.Append("AND b=?", 20)
	fmt.Println(s.ToSQL())
}
