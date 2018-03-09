package dbs

import (
	"fmt"
	"testing"
)

func TestBuilder(t *testing.T) {
	fmt.Println("===== RawBuilder =====")
	var b = NewBuilder()
	b.Append("SELECT a.id, a.name")
	b.Format("FROM %s AS a", "add")
	b.Append("WHERE id>?", 10)
	b.Append("LIMIT ?").Params(20)
	fmt.Println(b.ToSQL())
}

func TestLockBuilder(t *testing.T) {
	fmt.Println("===== LockBuilder =====")
	var b = NewLockBuilder()
	b.WriteLock("table1", "AS t1")
	b.WriteLock("table2", "AS t2")
	fmt.Println(b.ToSQL())
}