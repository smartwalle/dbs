package dbs

import (
	"github.com/smartwalle/xid"
)

var idGenerator IdGenerator

func init() {
	idGenerator, _ = xid.New()
}

type IdGenerator interface {
	Next() int64
}

func UseIdGenerator(g IdGenerator) {
	idGenerator = g
}

func GetIdGenerator() IdGenerator {
	return idGenerator
}

func Next() int64 {
	return idGenerator.Next()
}
