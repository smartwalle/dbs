package dbs

import (
	"database/sql/driver"
	"sync"
)

const (
	kDefaultByteSize = 1024
	kDefaultArgsSize = 128
)

var bPool *sync.Pool

func init() {
	bPool = &sync.Pool{
		New: func() interface{} {
			return NewBuffer()
		},
	}
}

func getBuffer() *Buffer {
	var bf = bPool.Get().(*Buffer)
	bf.Reset()
	bf.p = bPool
	return bf
}

type Writer interface {
	Write(p []byte) (n int, err error)

	WriteString(s string) (n int, err error)

	WriteArgs(args ...interface{})
}

func NewBuffer() *Buffer {
	return &Buffer{
		bs: make([]byte, 0, kDefaultByteSize),
		vs: make([]interface{}, 0, kDefaultArgsSize),
	}
}

type Buffer struct {
	p  *sync.Pool
	bs []byte
	vs []interface{}
}

func (this *Buffer) Write(bs []byte) (int, error) {
	this.bs = append(this.bs, bs...)
	return len(bs), nil
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

func (this *Buffer) WriteString(s string) (n int, err error) {
	return this.Write([]byte(s))
}

func (this *Buffer) Reset() {
	this.bs = this.bs[:0]
	this.vs = this.vs[:0]
}

func (this *Buffer) Values() []interface{} {
	var vs = make([]interface{}, 0, len(this.vs))
	vs = append(vs, this.vs...)
	return vs
}

func (this *Buffer) String() string {
	return string(this.bs)
}

func (this *Buffer) Release() {
	this.p.Put(this)
	this.p = nil
}
