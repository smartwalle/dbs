package dbs_test

import (
	"github.com/smartwalle/dbs"
	"testing"
)

func TestPostgresQL(t *testing.T) {
	dbs.UsePlaceholder(dbs.DollarPlaceholder)

	var sb = dbs.NewSelectBuilder()
	sb.Selects("u.id")
	sb.From("user", "AS u")
	sb.Where("u.id = ? OR u.id = ? OR u.id = ?", 10, 20, 30)

	check(t, sb, "SELECT u.id FROM user AS u WHERE u.id = $1 OR u.id = $2 OR u.id = $3", []interface{}{10, 20, 30})

	dbs.UsePlaceholder(dbs.QuestionPlaceholder)
}

func Benchmark_DollarPlaceholder(b *testing.B) {
	dbs.UsePlaceholder(dbs.DollarPlaceholder)
	for i := 0; i < b.N; i++ {
		var sb = dbs.NewSelectBuilder()
		sb.Selects("u.id")
		sb.From("user", "AS u")
		sb.Where("u.id = ? OR u.id = ? OR u.id = ?", 10, 20, 30)
		_, _, _ = sb.SQL()
	}
}
