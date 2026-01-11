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

	WriteArgument(flag uint8, arg interface{}) (err error)

	Arguments() []interface{}
}

var bufferPool = sync.Pool{
	New: func() interface{} {
		return &Buffer{
			Buffer:           bytes.NewBuffer(make([]byte, 0, kDefaultBufferSize)),
			arguments:        make([]interface{}, 0, kDefaultArgsSize),
			dialect:          GetDialect(),
			placeholderCount: 0,
		}
	},
}

type Buffer struct {
	*bytes.Buffer
	arguments        []interface{}
	dialect          Dialect
	placeholderCount int
}

func NewBuffer() *Buffer {
	var buffer = bufferPool.Get().(*Buffer)
	buffer.Buffer.Reset()
	buffer.arguments = buffer.arguments[:0]
	buffer.dialect = GetDialect()
	buffer.placeholderCount = 0
	return buffer
}

func (b *Buffer) Release() {
	bufferPool.Put(b)
}

func (b *Buffer) UseDialect(dialect Dialect) {
	if dialect == nil {
		dialect = GetDialect()
	}
	b.dialect = dialect
}

func (b *Buffer) WriteArgument(flag uint8, arg interface{}) (err error) {
	if flag&FlagArgument == FlagArgument {
		b.arguments = append(b.arguments, arg)
	}

	if flag&FlagPlaceholder == FlagPlaceholder {
		b.placeholderCount++
		if err = b.dialect.WritePlaceholder(b, b.placeholderCount); err != nil {
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
