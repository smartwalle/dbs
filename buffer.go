package dbs

import (
	"bytes"
	"sync"
)

const kDefaultArgsSize = 64
const kDefaultBufferSize = 1024

type Writer interface {
	Write(p []byte) (n int, err error)

	WriteByte(c byte) error

	WriteString(s string) (n int, err error)

	WriteArgs(args ...interface{})
}

var bufferPool = sync.Pool{
	New: func() interface{} {
		return &Buffer{
			Buffer: bytes.NewBuffer(make([]byte, 0, kDefaultBufferSize)),
			args:   make([]interface{}, 0, kDefaultArgsSize),
		}
	},
}

func getBuffer() *Buffer {
	var buffer = bufferPool.Get().(*Buffer)
	buffer.Reset()
	return buffer
}

func putBuffer(b *Buffer) {
	if b != nil {
		bufferPool.Put(b)
	}
}

type Buffer struct {
	*bytes.Buffer
	args []interface{}
}

func (b *Buffer) Reset() {
	b.args = b.args[:0]
	b.Buffer.Reset()
}

func (b *Buffer) WriteArgs(args ...interface{}) {
	b.args = append(b.args, args...)
}

func (b *Buffer) Args() []interface{} {
	var nArgs = make([]interface{}, 0, len(b.args))
	nArgs = append(nArgs, b.args...)
	return nArgs
}
