package dbs_test

import (
	"github.com/smartwalle/dbs"
	"testing"
)

func TestOnConflictKeyUpdate(t *testing.T) {
	var s = dbs.OnConflictKeyUpdate("k1", "k2")
	s.Append("k3=EXCLUDED.k3")
	s.Append("k4=?", "v4")

	check(t, s, "ON CONFLICT (k1, k2) DO UPDATE SET k3=EXCLUDED.k3, k4=?", []interface{}{"v4"})
}
