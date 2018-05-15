package dbs

import (
	"bytes"
	"strings"
	"fmt"
)

// --------------------------------------------------------------------------------
type Placeholder interface {
	Replace(sql string) (string, error)
}

var (
	Question = question{}
	Dollar   = dollar{}
)

// --------------------------------------------------------------------------------
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

		i++
		buf.WriteString(sql[:pos])
		fmt.Fprintf(buf, "$%d", i)
		sql = sql[pos+1:]
	}
	buf.WriteString(sql)
	return buf.String(), nil
}

// --------------------------------------------------------------------------------
func Placeholders(count int) string {
	if count <= 0 {
		return ""
	}
	return strings.Repeat(", ?", count)[2:]
}
