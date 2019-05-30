package dbs

import (
	"database/sql/driver"
	"sync"
)

// --------------------------------------------------------------------------------
var argsPool *sync.Pool

func init() {
	argsPool = &sync.Pool{
		New: func() interface{} {
			return newArgs()
		},
	}
}

func getArgs() *Args {
	var args = argsPool.Get().(*Args)
	args.Reset()
	return args
}

func releaseArgs(args *Args) {
	argsPool.Put(args)
}

// --------------------------------------------------------------------------------
type Args struct {
	values []interface{}
}

func (this *Args) Append(args ...interface{}) {
	for _, v := range args {
		switch vt := v.(type) {
		case driver.Valuer:
			v, _ := vt.Value()
			this.values = append(this.values, v)
		case SQLValue:
			this.values = append(this.values, vt.SQLValue())
		default:
			this.values = append(this.values, v)
		}
	}
}

func (this *Args) Reset() {
	this.values = this.values[:0]
}

func (this *Args) Values() []interface{} {
	return this.values
}

func newArgs() *Args {
	return &Args{values: make([]interface{}, 0, 32)}
}