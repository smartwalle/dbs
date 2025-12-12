package dbs_test

import (
	"github.com/smartwalle/dbs"
	"testing"
)

func TestSelectBuilder(t *testing.T) {
	var sb = dbs.NewSelectBuilder()
	sb.Selects("id", "user_id", "pay_no", "trade_no", "goods_id", "goods_name", "goods_price", "goods_cnt", "sku_id", "original_spec", "real_spec", "status", "created_at")
	sb.Table("order")
	sb.Where("id = ?", 123)
	sb.Where("status = ?", 1)
	t.Log(sb.SQL())
	t.Log(sb.Count().SQL())
}

func BenchmarkSelectBuilder(b *testing.B) {
	for i := 0; i < b.N; i++ {
		var sb = dbs.NewSelectBuilder()
		sb.Selects("id", "user_id", "pay_no", "trade_no", "goods_id", "goods_name", "goods_price", "goods_cnt", "sku_id", "original_spec", "real_spec", "status", "created_at")
		sb.Table("order")
		sb.Where("id = ?", "123")
		sb.Where("status = ?", 1)
		_, _, _ = sb.SQL()
	}
}
