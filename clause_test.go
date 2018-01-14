package dbs

import (
	"testing"
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
	//var w1 = NewWhere()
	////var w2 = NewWhere("w1=?", 20)
	////w2.prefix = "AND"
	////w1.Append(w2)
	//w1.Or("a=?", 10)
	//fmt.Println(w1.ToSQL(""))

	//var w2 = And(NC("w1=?", NC("ww=?", 10)), Or(NC("w2=?", 20), NC("w3=?", 30)))
	//fmt.Println(w2.ToSQL(""))

	//var wa = Or(And(C("w1=?", 10), C("w2=?", 20)), And(C("w3=?", 30), C("w4=?", 40)), Not(C("w5")))
	//fmt.Println(wa.ToSQL(""))
}