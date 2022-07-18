package dbs_test

import (
	"github.com/smartwalle/dbs"
	"testing"
)

func TestOnDuplicateKeyUpdate(t *testing.T) {
	var s = dbs.OnDuplicateKeyUpdate()
	s.Append("a = VALUES(a + ?)", 10)
	s.Append("b = VALUES(b)")

	check(t, s, "ON DUPLICATE KEY UPDATE a = VALUES(a + ?), b = VALUES(b)", []interface{}{10})
}
