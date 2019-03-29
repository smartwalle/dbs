package dbs

import (
	"bytes"
	"fmt"
	"strings"
)

var gDialect dialect = Default

func UseDialect(d dialect) {
	gDialect = d
}

// --------------------------------------------------------------------------------
type dialect interface {
	ParseVal(sql string) (string, error)
	Quote(s string) string
}

var (
	MySQL      = &mysql{}
	Default    = MySQL
	PostgreSQL = &postgresql{}
)

// --------------------------------------------------------------------------------
type mysql struct {
}

func (this *mysql) ParseVal(sql string) (string, error) {
	return sql, nil
}

func (this *mysql) Quote(s string) string {
	return fmt.Sprintf("`%s`", s)
}

// --------------------------------------------------------------------------------
type postgresql struct {
}

func (this *postgresql) ParseVal(sql string) (string, error) {
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

func (this *postgresql) Quote(s string) string {
	return fmt.Sprintf(`"%s"`, s)
}
