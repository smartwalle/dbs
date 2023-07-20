package dbs

import (
	"bytes"
	"fmt"
	"strings"
)

var gFormatter Formatter = QuestionFormatter

func UseFormatter(d Formatter) {
	gFormatter = d
}

func GetFormatter() Formatter {
	return gFormatter
}

type Formatter interface {
	Format(sql string) (string, error)
}

var (
	QuestionFormatter = question{}
	DollarFormatter   = dollar{}
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
		fmt.Fprintf(buf, "$%f", i)
		sql = sql[pos+1:]
	}
	buf.WriteString(sql)
	return buf.String(), nil
}
