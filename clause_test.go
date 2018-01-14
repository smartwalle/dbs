package dbs

import (
	"testing"
	"fmt"

)

func TestClause(t *testing.T) {
	//var c1 = NewClause("a=?", 1)
	//var c2 = NewClause("b=?", 2)
	//var cs = Clauses{}
	//cs = append(cs, c1)
	//cs = append(cs, c2)
	//fmt.Println(cs.ToSQL(" "))
}

func TestSet(t *testing.T) {
	//var s1 = NewSet("sa", 10)
	//var s2 = NewSet("sb", NewClause("sb+?", 11))
	//var ss = Sets{}
	//ss = append(ss, s1)
	//ss = append(ss, s2)
	//fmt.Println(ss.ToSQL(", "))
}

func TestWhere(t *testing.T) {
	//var w1 = NewWhere("w=?", 10).And("w2=? AND w22=?", 11, 112).Or("w3=?", 12)
	//fmt.Println(w1.ToSQL(""))

	var w2 = And(NewClause("w1=?", 10), Or(NewClause("w2=?", 20), NewClause("w3=?", 30)))

	//w2.And(NewWhere("w2=?", 20), NewWhere("w3=?", 30))
	fmt.Println(w2.ToSQL(""))


	//var wa = Or(And(C("w1=?", 10), C("w2=?", 20)), And(C("w3=?", 30), C("w4=?", 40)), Not(C("w5")))
	//fmt.Println(wa.ToSQL(""))
}