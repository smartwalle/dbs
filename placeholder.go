package dbs

import (
	"bytes"
	"strconv"
	"strings"
	"sync"
)

var gPlaceholder Placeholder = QuestionPlaceholder

func UsePlaceholder(placeholder Placeholder) {
	if placeholder != nil {
		gPlaceholder = placeholder
	}
}

func GetPlaceholder() Placeholder {
	return gPlaceholder
}

type Placeholder interface {
	Replace(clause string) (string, error)
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

func (q *question) Replace(clause string) (string, error) {
	return clause, nil
}

type dollar struct {
	pool sync.Pool
}

func (d *dollar) Replace(clause string) (string, error) {
	var buf = d.pool.Get().(*bytes.Buffer)
	buf.Reset()
	var i = 0

	for {
		pos := strings.Index(clause, "?")
		if pos == -1 {
			break
		}

		if len(clause[pos:]) > 1 && clause[pos:pos+2] == "??" {
			buf.WriteString(clause[:pos])
			buf.WriteString("?")
			clause = clause[pos+2:]
			continue
		}

		i++
		buf.WriteString(clause[:pos])

		buf.WriteString("$")
		buf.WriteString(strconv.Itoa(i))

		clause = clause[pos+1:]
	}
	buf.WriteString(clause)
	var s = buf.String()
	d.pool.Put(buf)
	return s, nil
}
