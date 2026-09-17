package dbs

import "time"

type Dialect interface {
	UseTimeLocation(location *time.Location)

	WritePlaceholder(w Writer, idx int) error

	WriteArgument(w Writer, arg any) error
}

type ExplainValuer interface {
	ExplainValue() (any, error)
}
