package dbs

import (
	"fmt"
	"testing"
)

func TestOnDuplicateKeyUpdate(t *testing.T) {
	var dup = OnDuplicateKeyUpdate()
	dup.Append("a = VALUES(a + ?)", 10)
	dup.Append("b = VALUES(b)")

	fmt.Println(dup.ToSQL())
}
