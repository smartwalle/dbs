package dbs

import "time"

type Dialect interface {
	UseTimeLocation(location *time.Location)

	WriteIdentifier(w Writer, s string) error

	WritePlaceholder(w Writer, idx int) error

	WriteArgument(w Writer, arg any) error
}

type ExplainValuer interface {
	ExplainValue() (string, error)
}
