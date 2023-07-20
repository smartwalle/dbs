package dbs

import (
	"bytes"
	"fmt"
	"strings"
)

var gDialect Dialect = Question

func UseDialect(d Dialect) {
	gDialect = d
}

func GetDialect() Dialect {
	return gDialect
}

type Dialect interface {
	Format(sql string) (string, error)
}

var (
	Question = question{}
	Dollar   = dollar{}
)

type question struct {
}

func (this question) Format(sql string) (string, error) {
	return sql, nil
}

type dollar struct {
}

func (this dollar) Format(sql string) (string, error) {
	var buf = &bytes.Buffer{}
	var i = 0

	for {
		pos := strings.Index(sql, "?")
		if pos == -1 {
			break
		}

		i++
		buf.WriteString(sql[:pos])
		fmt.Fprintf(buf, "$%d", i)
		sql = sql[pos+1:]
	}
	buf.WriteString(sql)
	return buf.String(), nil
}
