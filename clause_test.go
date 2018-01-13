package dbs

import (
	"testing"
	"fmt"
)

func TestClause(t *testing.T) {
	var c1 = NewClause("a=?", 1)
	var c2 = NewClause("b=?", 2)

	var cs = Clauses{}
	cs = append(cs, c1)
	cs = append(cs, c2)

	fmt.Println(cs.ToSQL())



	var s1 = NewSet("sa", 10)
	var s2 = NewSet("sb", NewClause("sb+?", 11))
	var ss = Sets{}
	ss = append(ss, s1)
	ss = append(ss, s2)
	fmt.Println(ss.ToSQL())
}
