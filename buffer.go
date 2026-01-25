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
	UseDialect(p Dialect)

	Write(p []byte) (n int, err error)

	WriteByte(c byte) error

	WriteString(s string) (n int, err error)

	WriteArgument(flag uint8, arg any) (err error)

	Arguments() []any
}

var bufferPool = sync.Pool{
	New: func() interface{} {
		return &Buffer{
			Buffer:           bytes.NewBuffer(make([]byte, 0, kDefaultBufferSize)),
			arguments:        make([]any, 0, kDefaultArgsSize),
			placeholderCount: 0,
		}
	},
}

type Buffer struct {
	*bytes.Buffer
	arguments        []any
	dialect          Dialect
	placeholderCount int
}

func NewBuffer() *Buffer {
	var buffer = bufferPool.Get().(*Buffer)
	buffer.Buffer.Reset()
	buffer.arguments = buffer.arguments[:0]
	buffer.dialect = nil
	buffer.placeholderCount = 0
	return buffer
}

func (b *Buffer) Release() {
	bufferPool.Put(b)
}

func (b *Buffer) UseDialect(dialect Dialect) {
	b.dialect = dialect
}

func (b *Buffer) WriteArgument(flag uint8, arg any) (err error) {
	if flag&FlagArgument == FlagArgument {
		b.arguments = append(b.arguments, arg)
	}

	if flag&FlagPlaceholder == FlagPlaceholder {
		b.placeholderCount++
		if b.dialect != nil {
			if err = b.dialect.WritePlaceholder(b, b.placeholderCount); err != nil {
				return err
			}
		} else {
			if err = b.WriteByte('?'); err != nil {
				return err
			}
		}
	}
	return nil
}

func (b *Buffer) Arguments() []any {
	var args = make([]any, len(b.arguments))
	copy(args, b.arguments)
	return args
}
