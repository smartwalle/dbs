package dbs_test

import (
	"github.com/smartwalle/dbs"
	"testing"
)

func TestIntersectBuilder(t *testing.T) {
	var s1 = dbs.NewSelectBuilder()
	s1.Selects("p1.id", "p1.name", "p1.price")
	s1.From("products AS p1")

	var s2 = dbs.NewSelectBuilder()
	s2.Selects("p2.id", "p2.name", "p2.price")
	s2.From("products AS p2")

	var ub = dbs.NewIntersectBuilder()
	ub.Intersect(s1, s2)
	ub.OrderBy("price")
	t.Log(ub.SQL())

	check(
		t,
		ub,
		"(SELECT p1.id, p1.name, p1.price FROM products AS p1) INTERSECT (SELECT p2.id, p2.name, p2.price FROM products AS p2) ORDER BY price",
		[]interface{}{},
	)
}
