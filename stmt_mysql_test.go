package dbs

import (
	"testing"
)

func TestOnDuplicateKeyUpdate(t *testing.T) {
	var s = OnDuplicateKeyUpdate()
	s.Append("a = VALUES(a + ?)", 10)
	s.Append("b = VALUES(b)")

	t.Log(s.ToSQL())
}
