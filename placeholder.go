package dbs

import (
	"strconv"
	"strings"
)

type Placeholder interface {
	Replace(sql string) (string, error)
}

var QuestionPlaceholder = question{}

var DollarPlaceholder = dollar{}

type question struct {
}

func (q question) Replace(sql string) (string, error) {
	return sql, nil
}

type dollar struct {
}

func (d dollar) Replace(sql string) (string, error) {
	return replace(sql, "$")
}

func replace(sql string, prefix string) (string, error) {
	var buf = getBuffer()
	defer putBuffer(buf)

	var i = 0
	var err error

	for {
		pos := strings.Index(sql, "?")
		if pos == -1 {
			break
		}

		if len(sql[pos:]) > 1 && sql[pos:pos+2] == "??" {
			if _, err = buf.WriteString(sql[:pos]); err != nil {
				return "", err
			}
			if _, err = buf.WriteString("?"); err != nil {
				return "", err
			}
			sql = sql[pos+2:]
			continue
		}

		i++
		if _, err = buf.WriteString(sql[:pos]); err != nil {
			return "", err
		}
		if _, err = buf.WriteString(prefix); err != nil {
			return "", err
		}
		if _, err = buf.WriteString(strconv.Itoa(i)); err != nil {
			return "", err
		}
		sql = sql[pos+1:]
	}
	if _, err = buf.WriteString(sql); err != nil {
		return "", err
	}
	return buf.String(), nil
}
