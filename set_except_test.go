package dbs_test

import (
	"github.com/smartwalle/dbs"
	"testing"
)

func TestExceptBuilder(t *testing.T) {
	var s1 = dbs.NewSelectBuilder()
	s1.Selects("p1.id", "p1.name", "p1.price")
	s1.From("products AS p1")

	var s2 = dbs.NewSelectBuilder()
	s2.Selects("p2.id", "p2.name", "p2.price")
	s2.From("products AS p2")

	var eb = dbs.NewExceptBuilder()
	eb.Except(s1, s2)
	eb.OrderBy("price")

	check(
		t,
		eb,
		"(SELECT p1.id, p1.name, p1.price FROM products AS p1) EXCEPT (SELECT p2.id, p2.name, p2.price FROM products AS p2) ORDER BY price",
		[]interface{}{},
	)
}
