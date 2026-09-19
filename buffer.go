package dbs

import (
	"bytes"
	"errors"
	"sync"
)

var ErrInvalidDialect = errors.New("dbs: invalid dialect")

const kDefaultArgsSize = 16
const kDefaultBufferSize = 2048

type Writer interface {
	UseDialect(p Dialect)

	UseInline(inline bool)

	Arguments() []any

	WriteArgument(arg any) (err error)

	Write(p []byte) (n int, err error)

	WriteByte(c byte) error

	WriteString(s string) (n int, err error)
}

var bufferPool = sync.Pool{
	New: func() any {
		return &Buffer{
			Buffer:           bytes.NewBuffer(make([]byte, 0, kDefaultBufferSize)),
			inline:           false,
			arguments:        make([]any, 0, kDefaultArgsSize),
			placeholderCount: 0,
		}
	},
}

type Buffer struct {
	*bytes.Buffer
	inline           bool
	arguments        []any
	dialect          Dialect
	placeholderCount int
}

func NewBuffer() *Buffer {
	var buffer = bufferPool.Get().(*Buffer)
	buffer.Buffer.Reset()
	buffer.inline = false
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

func (b *Buffer) UseInline(inline bool) {
	b.inline = inline
}

func (b *Buffer) Arguments() []any {
	var args = make([]any, len(b.arguments))
	copy(args, b.arguments)
	return args
}

func (b *Buffer) WriteIdentifier(s string) (err error) {
	if s == "" {
		return nil
	}
	if b.dialect != nil {
		return b.dialect.WriteIdentifier(b, s)
	}
	if _, err = b.WriteString(s); err != nil {
		return err
	}
	return nil
}

func (b *Buffer) WriteArgument(arg any) (err error) {
	if b.inline {
		if b.dialect == nil {
			return ErrInvalidDialect
		}
		return b.dialect.WriteArgument(b, arg)
	}

	b.arguments = append(b.arguments, arg)

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
	return nil
}
