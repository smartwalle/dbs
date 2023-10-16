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

func (buffer *Buffer) WriteArgs(args ...interface{}) {
	for _, arg := range args {
		switch argType := arg.(type) {
		case driver.Valuer:
			v, _ := argType.Value()
			buffer.vs = append(buffer.vs, v)
		case SQLValue:
			buffer.vs = append(buffer.vs, argType.SQLValue())
		default:
			buffer.vs = append(buffer.vs, arg)
		}
	}
}

func (buffer *Buffer) Reset() {
	buffer.vs = buffer.vs[:0]
	buffer.Buffer.Reset()
}

func (buffer *Buffer) Values() []interface{} {
	var vs = make([]interface{}, 0, len(buffer.vs))
	vs = append(vs, buffer.vs...)
	return vs
}

func (buffer *Buffer) Release() {
	bPool.Put(buffer)
}
