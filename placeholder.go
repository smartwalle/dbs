package dbs

import (
	"bytes"
	"strconv"
	"strings"
	"sync"
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
	QuestionPlaceholder = &question{}
	DollarPlaceholder   = &dollar{
		pool: sync.Pool{New: func() interface{} {
			return &bytes.Buffer{}
		}},
	}
)

type question struct {
}

func (q *question) Replace(sql string) (string, error) {
	return sql, nil
}

type dollar struct {
	pool sync.Pool
}

func (d *dollar) Replace(sql string) (string, error) {
	var buf = d.pool.Get().(*bytes.Buffer)
	buf.Reset()
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

		buf.WriteString("$")
		buf.WriteString(strconv.Itoa(i))

		sql = sql[pos+1:]
	}
	buf.WriteString(sql)
	var s = buf.String()
	d.pool.Put(buf)
	return s, nil
}
