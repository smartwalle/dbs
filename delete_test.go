package dbs_test

import (
	"testing"

	"github.com/smartwalle/dbs"
)

func TestDeleteBuilder(t *testing.T) {
	var rb = dbs.NewDeleteBuilder()
	rb.Table("user")
	rb.Where("id = ?", 10)
	t.Log(rb.SQL())
}

func BenchmarkDeleteBuilder(b *testing.B) {
	for i := 0; i < b.N; i++ {
		var rb = dbs.NewDeleteBuilder()
		rb.Table("user")
		rb.Where("id = ?", 10)
		_, _, _ = rb.SQL()
	}
}
