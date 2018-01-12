package dbs

import (
	"testing"
	"fmt"
)

func TestAND(t *testing.T) {
	var and = AND(C("a=?", 1), C("b=?", 2))
	fmt.Println(and.ToSQL())

	and = AND(OR(C("a=?", 1), C("b=?", 2)), OR(C("a=?",1), C("c=?", 2)))
	fmt.Println(and.ToSQL())

	fmt.Println(C("a1=10").ToSQL())
}