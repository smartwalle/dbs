package dbs

import (
	"testing"
)

func TestOnConflictKeyUpdate(t *testing.T) {
	var s = OnConflictKeyUpdate("k1", "k2")
	s.Append("k3=EXCLUDED.k3")
	s.Append("k4=?", "v4")
	t.Log(s.ToSQL())
}
