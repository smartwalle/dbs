package dbs_test

import (
	"github.com/smartwalle/dbs"
	"sync"
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
		sb.Selects("id, user_id, pay_no, trade_no, goods_id, goods_name, goods_price, goods_cnt, sku_id, original_spec, real_spec, status, created_at")
		sb.Table("order")
		sb.Where("id = ? AND status = ?", "123", 1)
		_, _, _ = sb.SQL()
	}
}

var pool = sync.Pool{
	New: func() interface{} {
		return dbs.NewSelectBuilder()
	},
}

func BenchmarkSelectBuilder_Pool(b *testing.B) {

	for i := 0; i < b.N; i++ {
		var sb = pool.Get().(*dbs.SelectBuilder)
		sb.Reset()
		sb.Selects("id, user_id, pay_no, trade_no, goods_id, goods_name, goods_price, goods_cnt, sku_id, original_spec, real_spec, status, created_at")
		sb.Table("order")
		sb.Where("id = ? AND status = ?", "123", 1)
		_, _, _ = sb.SQL()

		pool.Put(sb)
	}
}
