package dbs

import (
	"bytes"
	"sync"
)

const kDefaultArgsSize = 16
const kDefaultBufferSize = 1024

const (
	FlagPlaceholder = uint8(1)
	FlagArgument    = uint8(2)
)

type Writer interface {
	UsePlaceholder(p Placeholder)

	Write(p []byte) (n int, err error)

	WriteByte(c byte) error

	WriteString(s string) (n int, err error)

	WriteArgument(flag uint8, arg interface{}) (err error)

	Arguments() []interface{}
}

var bufferPool = sync.Pool{
	New: func() interface{} {
		return &Buffer{
			Buffer:           bytes.NewBuffer(make([]byte, 0, kDefaultBufferSize)),
			arguments:        make([]interface{}, 0, kDefaultArgsSize),
			placeholder:      GlobalPlaceholder(),
			placeholderCount: 0,
		}
	},
}

func getBuffer() *Buffer {
	var buffer = bufferPool.Get().(*Buffer)
	buffer.Buffer.Reset()
	buffer.arguments = buffer.arguments[:0]
	buffer.placeholderCount = 0
	return buffer
}

func putBuffer(b *Buffer) {
	if b != nil {
		bufferPool.Put(b)
	}
}

type Buffer struct {
	*bytes.Buffer
	arguments        []interface{}
	placeholder      Placeholder
	placeholderCount int
}

func (b *Buffer) UsePlaceholder(p Placeholder) {
	if p == nil {
		p = GlobalPlaceholder()
	}
	b.placeholder = p
}

func (b *Buffer) WriteArgument(flag uint8, arg interface{}) (err error) {
	if flag&FlagArgument == FlagArgument {
		b.arguments = append(b.arguments, arg)
	}

	if flag&FlagPlaceholder == FlagPlaceholder {
		b.placeholderCount++
		if err = b.placeholder.WriteTo(b, b.placeholderCount); err != nil {
			return err
		}
	}
	return nil
}

func (b *Buffer) Arguments() []interface{} {
	var args = make([]interface{}, len(b.arguments))
	copy(args, b.arguments)
	return args
}
