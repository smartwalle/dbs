package dbs

type Dialect interface {
	WritePlaceholder(w Writer, idx int) error
}
