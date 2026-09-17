package dbs_test

import (
	"testing"
	"time"

	"github.com/smartwalle/dbs"
	"github.com/smartwalle/dbs/dialect/postgres"
)

func TestInsertBuilder_SQL(t *testing.T) {
	var ib = dbs.NewInsertBuilder()
	ib.Table("user")
	ib.Columns("id", "name", "status", "age")
	ib.Values(1, "Sample", 2, 10)
	ib.Values(2, "Sample", 2, 10)
	ib.Values(3, "Sample", 2, 10)
	ib.Values(4, "Sample", 2, 10)
	t.Log(ib.SQL())
}

func TestInsertBuilder_Explain(t *testing.T) {
	var ib = dbs.NewInsertBuilder()
	ib.UseDialect(postgres.Dialect())
	ib.Table("user")
	ib.Columns("t1", "t2")
	ib.Values(time.Now(), time.Now())
	t.Log(ib.Explain())
}

func BenchmarkInsertBuilder(b *testing.B) {
	for i := 0; i < b.N; i++ {
		var ib = dbs.NewInsertBuilder()
		ib.Table("user")
		ib.Columns("id", "name", "status", "age")
		ib.Values(1, "Sample", 2, 10)
		ib.Values(2, "Sample", 2, 10)
		ib.Values(3, "Sample", 2, 10)
		ib.Values(4, "Sample", 2, 10)
		_, _, _ = ib.SQL()
	}
}
