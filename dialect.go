package dbs

type Dialect interface {
	WritePlaceholder(w Writer, idx int) error

	WriteArgument(w Writer, arg any) error
}
