package dbs

import (
	"bytes"
	"database/sql/driver"
	"sync"
)

const (
	kDefaultArgsSize = 64
)

var bPool sync.Pool

func init() {
	bPool = sync.Pool{
		New: func() interface{} {
			return NewBuffer()
		},
	}
}

func getBuffer() *Buffer {
	var bf = bPool.Get().(*Buffer)
	bf.Reset()
	return bf
}

type Writer interface {
	Write(p []byte) (n int, err error)

	WriteString(s string) (n int, err error)

	WriteArgs(args ...interface{})
}

func NewBuffer() *Buffer {
	return &Buffer{
		vs: make([]interface{}, 0, kDefaultArgsSize),
	}
}

type Buffer struct {
	bytes.Buffer
	vs []interface{}
}

func (this *Buffer) WriteArgs(args ...interface{}) {
	for _, v := range args {
		switch vt := v.(type) {
		case driver.Valuer:
			v, _ := vt.Value()
			this.vs = append(this.vs, v)
		case SQLValue:
			this.vs = append(this.vs, vt.SQLValue())
		default:
			this.vs = append(this.vs, v)
		}
	}
}

func (this *Buffer) Reset() {
	this.vs = this.vs[:0]
	this.Buffer.Reset()
}

func (this *Buffer) Values() []interface{} {
	var vs = make([]interface{}, 0, len(this.vs))
	vs = append(vs, this.vs...)
	return vs
}

func (this *Buffer) Release() {
	bPool.Put(this)
}
