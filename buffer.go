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

	WritePlaceholder() error

	WriteArguments(args ...interface{})
}

var bufferPool = sync.Pool{
	New: func() interface{} {
		return &Buffer{
			Buffer:       bytes.NewBuffer(make([]byte, 0, kDefaultBufferSize)),
			arguments:    make([]interface{}, 0, kDefaultArgsSize),
			placeholder:  globalPlaceholder,
			replaceCount: 0,
		}
	},
}

func getBuffer() *Buffer {
	var buffer = bufferPool.Get().(*Buffer)
	buffer.Buffer.Reset()
	buffer.arguments = buffer.arguments[:0]
	buffer.replaceCount = 0
	return buffer
}

func putBuffer(b *Buffer) {
	if b != nil {
		bufferPool.Put(b)
	}
}

type Buffer struct {
	*bytes.Buffer
	arguments    []interface{}
	placeholder  Placeholder
	replaceCount int
}

func (b *Buffer) UsePlaceholder(p Placeholder) {
	if p == nil {
		p = globalPlaceholder
	}
	b.placeholder = p
}

func (b *Buffer) WritePlaceholder() error {
	b.replaceCount++
	if _, err := b.Buffer.WriteString(b.placeholder.Replace(b.replaceCount)); err != nil {
		return err
	}
	return nil
}

func (b *Buffer) WriteArguments(args ...interface{}) {
	b.arguments = append(b.arguments, args...)
}

func (b *Buffer) Arguments() []interface{} {
	var args = make([]interface{}, 0, len(b.arguments))
	args = append(args, b.arguments...)
	return args
}
