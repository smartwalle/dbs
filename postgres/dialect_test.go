package postgres_test

import (
	"github.com/smartwalle/dbs"
	"github.com/smartwalle/dbs/postgres"
	"testing"
)

func BenchmarkClause_SQL(b *testing.B) {
	dbs.UseDialect(postgres.Dialect())
	for i := 0; i < b.N; i++ {
		var c1 = dbs.SQL("SELECT * FROM user u WHERE u.id = ? AND u.gender = ? ", "1", 2)
		_, _, _ = c1.SQL()
	}
}
