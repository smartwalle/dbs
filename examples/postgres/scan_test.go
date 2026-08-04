package postgres_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/smartwalle/dbs"
)

const (
	selectScalar         = "SELECT id FROM dbs_example_mail WHERE id = 1"
	selectScalarBool     = "SELECT TRUE AS id FROM dbs_example_mail WHERE id = 1"
	selectScalarList     = "SELECT id FROM dbs_example_mail WHERE id < 5 ORDER BY id ASC"
	selectScalarBoolList = "SELECT id % 2 = 0 AS id FROM dbs_example_mail WHERE id < 5 ORDER BY id ASC"
	selectMail           = "SELECT id, email, status, created_at, updated_at, extra FROM dbs_example_mail WHERE id = 1"
	selectMailList       = "SELECT id, email, status, created_at, updated_at, extra FROM dbs_example_mail WHERE id < 5 ORDER BY id ASC"
)

func TestScanScalar(t *testing.T) {
	db := open(t)

	runScanScalar[string](t, db, "string", selectScalar)
	runScanScalar[int](t, db, "int", selectScalar)
	runScanScalar[int8](t, db, "int8", selectScalar)
	runScanScalar[int16](t, db, "int16", selectScalar)
	runScanScalar[int32](t, db, "int32", selectScalar)
	runScanScalar[int64](t, db, "int64", selectScalar)
	runScanScalar[uint](t, db, "uint", selectScalar)
	runScanScalar[uint8](t, db, "uint8", selectScalar)
	runScanScalar[uint16](t, db, "uint16", selectScalar)
	runScanScalar[uint32](t, db, "uint32", selectScalar)
	runScanScalar[uint64](t, db, "uint64", selectScalar)
	runScanScalar[float32](t, db, "float32", selectScalar)
	runScanScalar[float64](t, db, "float64", selectScalar)
	runScanScalar[bool](t, db, "bool", selectScalarBool)
	runScanScalar[Mail](t, db, "Mail", selectMail)
	runScanScalar[*Mail](t, db, "*Mail", selectMail)
}

func TestScanSlice(t *testing.T) {
	db := open(t)

	runScanSlice[[]string](t, db, "[]string", selectScalarList)
	runScanSlice[*[]string](t, db, "*[]string", selectScalarList)
	runScanSlice[[]int](t, db, "[]int", selectScalarList)
	runScanSlice[*[]int](t, db, "*[]int", selectScalarList)
	runScanSlice[[]int8](t, db, "[]int8", selectScalarList)
	runScanSlice[*[]int8](t, db, "*[]int8", selectScalarList)
	runScanSlice[[]int16](t, db, "[]int16", selectScalarList)
	runScanSlice[*[]int16](t, db, "*[]int16", selectScalarList)
	runScanSlice[[]int32](t, db, "[]int32", selectScalarList)
	runScanSlice[*[]int32](t, db, "*[]int32", selectScalarList)
	runScanSlice[[]int64](t, db, "[]int64", selectScalarList)
	runScanSlice[*[]int64](t, db, "*[]int64", selectScalarList)
	runScanSlice[[]uint](t, db, "[]uint", selectScalarList)
	runScanSlice[*[]uint](t, db, "*[]uint", selectScalarList)
	runScanSlice[[]uint8](t, db, "[]uint8", selectScalarList)
	runScanSlice[*[]uint8](t, db, "*[]uint8", selectScalarList)
	runScanSlice[[]uint16](t, db, "[]uint16", selectScalarList)
	runScanSlice[*[]uint16](t, db, "*[]uint16", selectScalarList)
	runScanSlice[[]uint32](t, db, "[]uint32", selectScalarList)
	runScanSlice[*[]uint32](t, db, "*[]uint32", selectScalarList)
	runScanSlice[[]uint64](t, db, "[]uint64", selectScalarList)
	runScanSlice[*[]uint64](t, db, "*[]uint64", selectScalarList)
	runScanSlice[[]float32](t, db, "[]float32", selectScalarList)
	runScanSlice[*[]float32](t, db, "*[]float32", selectScalarList)
	runScanSlice[[]float64](t, db, "[]float64", selectScalarList)
	runScanSlice[*[]float64](t, db, "*[]float64", selectScalarList)
	runScanSlice[[]bool](t, db, "[]bool", selectScalarBoolList)
	runScanSlice[*[]bool](t, db, "*[]bool", selectScalarBoolList)
	runScanSlice[[]Mail](t, db, "[]Mail", selectMailList)
	runScanSlice[[]*Mail](t, db, "[]*Mail", selectMailList)
	runScanSlice[*[]Mail](t, db, "*[]Mail", selectMailList)
	runScanSlice[*[]*Mail](t, db, "*[]*Mail", selectMailList)
}

func TestScanMap(t *testing.T) {
	db := open(t)

	runScanMap[any](t, db, "map[string]any", selectScalar)
	runScanMap[string](t, db, "map[string]string", selectScalar)
	runScanMap[int](t, db, "map[string]int", selectScalar)
	runScanMap[int8](t, db, "map[string]int8", selectScalar)
	runScanMap[int16](t, db, "map[string]int16", selectScalar)
	runScanMap[int32](t, db, "map[string]int32", selectScalar)
	runScanMap[int64](t, db, "map[string]int64", selectScalar)
	runScanMap[uint](t, db, "map[string]uint", selectScalar)
	runScanMap[uint8](t, db, "map[string]uint8", selectScalar)
	runScanMap[uint16](t, db, "map[string]uint16", selectScalar)
	runScanMap[uint32](t, db, "map[string]uint32", selectScalar)
	runScanMap[uint64](t, db, "map[string]uint64", selectScalar)
	runScanMap[float32](t, db, "map[string]float32", selectScalar)
	runScanMap[float64](t, db, "map[string]float64", selectScalar)
	runScanMap[bool](t, db, "map[string]bool", selectScalarBool)
}

func TestScanMapSlice(t *testing.T) {
	db := open(t)

	runScanMapSlice[any](t, db, "[]map[string]any", selectScalarList)
	runScanMapSlice[string](t, db, "[]map[string]string", selectScalarList)
	runScanMapSlice[int](t, db, "[]map[string]int", selectScalarList)
	runScanMapSlice[int8](t, db, "[]map[string]int8", selectScalarList)
	runScanMapSlice[int16](t, db, "[]map[string]int16", selectScalarList)
	runScanMapSlice[int32](t, db, "[]map[string]int32", selectScalarList)
	runScanMapSlice[int64](t, db, "[]map[string]int64", selectScalarList)
	runScanMapSlice[uint](t, db, "[]map[string]uint", selectScalarList)
	runScanMapSlice[uint8](t, db, "[]map[string]uint8", selectScalarList)
	runScanMapSlice[uint16](t, db, "[]map[string]uint16", selectScalarList)
	runScanMapSlice[uint32](t, db, "[]map[string]uint32", selectScalarList)
	runScanMapSlice[uint64](t, db, "[]map[string]uint64", selectScalarList)
	runScanMapSlice[float32](t, db, "[]map[string]float32", selectScalarList)
	runScanMapSlice[float64](t, db, "[]map[string]float64", selectScalarList)
	runScanMapSlice[bool](t, db, "[]map[string]bool", selectScalarBoolList)
}

func runScanScalar[T any](t *testing.T, db dbs.Database, name, query string) {
	t.Helper()

	t.Run(name, func(t *testing.T) {
		value := scanScalar[T](t, db, query, true)
		if reflect.TypeOf(value) == nil {
			t.Fatal("scan result type is nil")
		}
	})
}

func runScanSlice[T any](t *testing.T, db dbs.Database, name, query string) {
	t.Helper()

	t.Run(name, func(t *testing.T) {
		value := scanSlice[T](t, db, query, true)
		if reflect.TypeOf(value) == nil {
			t.Fatal("scan result type is nil")
		}
	})
}

func runScanMap[T any](t *testing.T, db dbs.Database, name, query string) {
	t.Helper()

	t.Run(name, func(t *testing.T) {
		value := scanMap[T](t, db, query, true)
		if len(value) == 0 {
			t.Fatal("map is empty")
		}
	})
}

func runScanMapSlice[T any](t *testing.T, db dbs.Database, name, query string) {
	t.Helper()

	t.Run(name, func(t *testing.T) {
		value := scanMapSlice[T](t, db, query, true)
		if len(value) == 0 {
			t.Fatal("map slice is empty")
		}
	})
}

func scanScalar[T any](t testing.TB, db dbs.Database, query string, enableLog bool) T {
	t.Helper()

	value, err := dbs.Query[T](context.Background(), db, query)
	if err != nil {
		t.Fatal(err)
	}
	if enableLog {
		t.Logf("%T: %+v", value, value)
	}
	return value
}

func scanSlice[T any](t testing.TB, db dbs.Database, query string, enableLog bool) T {
	t.Helper()

	value, err := dbs.Query[T](context.Background(), db, query)
	if err != nil {
		t.Fatal(err)
	}
	if enableLog {
		t.Logf("%T: %+v", value, value)
	}
	return value
}

func scanMap[T any](t testing.TB, db dbs.Database, query string, enableLog bool) map[string]T {
	t.Helper()

	value, err := dbs.Query[map[string]T](context.Background(), db, query)
	if err != nil {
		t.Fatal(err)
	}
	if enableLog {
		t.Logf("%T: %+v", value, value)
	}
	return value
}

func scanMapSlice[T any](t testing.TB, db dbs.Database, query string, enableLog bool) []map[string]T {
	t.Helper()

	value, err := dbs.Query[[]map[string]T](context.Background(), db, query)
	if err != nil {
		t.Fatal(err)
	}
	if enableLog {
		t.Logf("%T: %+v", value, value)
	}
	return value
}
