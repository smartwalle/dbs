package postgres_test

import "testing"

func BenchmarkScanScalar(b *testing.B) {
	db := open(b)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		scanScalar[int64](b, db, selectScalar, false)
	}
}

func BenchmarkScanSlice(b *testing.B) {
	db := open(b)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		scanSlice[[]int64](b, db, selectScalarList, false)
	}
}

func BenchmarkScanMap(b *testing.B) {
	db := open(b)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		scanMap[int64](b, db, selectScalar, false)
	}
}

func BenchmarkScanMapSlice(b *testing.B) {
	db := open(b)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		scanMapSlice[any](b, db, selectScalarList, false)
	}
}
