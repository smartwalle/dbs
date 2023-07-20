package dbs

import (
	"bytes"
	"fmt"
	"strings"
)

var gPlaceholder Placeholder = QuestionPlaceholder

func UsePlaceholder(d Placeholder) {
	gPlaceholder = d
}

func GetPlaceholder() Placeholder {
	return gPlaceholder
}

type Placeholder interface {
	Replace(sql string) (string, error)
}

var (
	QuestionPlaceholder = question{}
	DollarPlaceholder   = dollar{}
)

type question struct {
}

func (this question) Replace(sql string) (string, error) {
	return sql, nil
}

type dollar struct {
}

func (this dollar) Replace(sql string) (string, error) {
	var buf = &bytes.Buffer{}
	var i = 0

	for {
		pos := strings.Index(sql, "?")
		if pos == -1 {
			break
		}

		if len(sql[pos:]) > 1 && sql[pos:pos+2] == "??" {
			buf.WriteString(sql[:pos])
			buf.WriteString("?")
			sql = sql[pos+2:]
			continue
		}

		i++
		buf.WriteString(sql[:pos])
		fmt.Fprintf(buf, "$%d", i)
		sql = sql[pos+1:]
	}
	buf.WriteString(sql)
	return buf.String(), nil
}
