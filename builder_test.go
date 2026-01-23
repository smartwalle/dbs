package dbs_test

import (
	"testing"

	"github.com/smartwalle/dbs"
)

func TestBuilder(t *testing.T) {
	var rb = dbs.NewBuilder()
	rb.Raw("SELECT * FROM user")
	rb.Raw("WHERE id = ?", 10)
	t.Log(rb.SQL())
}
