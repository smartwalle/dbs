package dbs

import "strings"

type CountBuilder struct {
	*SelectBuilder
}

func NewCountBuilder(sb *SelectBuilder, args ...string) *CountBuilder {
	var ts []string
	ts = append(ts, "COUNT(*)")

	if len(args) > 0 {
		ts = append(ts, args...)
	}

	var cb = &CountBuilder{}
	cb.SelectBuilder = sb.Clone()
	cb.columns = statements{NewStatement(strings.Join(ts, " "))}
	cb.limit = nil
	cb.offset = nil
	return cb
}
