package dba

import (
	"io"
	"strings"
)

type expression struct {
	sql  string
	args []interface{}
}

type expressions []expression

func Expression(sql string, args ...interface{}) expression {
	return expression{sql, args}
}

func (this expressions) appendToSQL(w io.Writer, sep string, args []interface{}) ([]interface{}, error) {
	for i, e := range this {
		if i > 0 {
			_, err := io.WriteString(w, sep)
			if err != nil {
				return nil, err
			}
		}
		_, err := io.WriteString(w, e.sql)
		if err != nil {
			return nil, err
		}
		args = append(args, e.args...)
	}
	return args, nil
}

type whereExpression struct {
	sql  string
	args []interface{}
}

type whereExpressions []whereExpression


func WhereExpression(sql string, args ...interface{}) whereExpression {
	return whereExpression{sql, args}
}

func (this whereExpressions) appendToSQL(w io.Writer, sep string, args []interface{}) ([]interface{}, error) {
	for i, e := range this {
		if i > 0 {
			_, err := io.WriteString(w, sep)
			if err != nil {
				return nil, err
			}
		}
		if i == 0 {
			e.sql = strings.TrimSpace(e.sql)
			if strings.HasPrefix(e.sql, "AND") {
				e.sql = strings.TrimPrefix(e.sql, "AND")
			}
			if strings.HasPrefix(e.sql, "OR") {
				e.sql = strings.TrimPrefix(e.sql, "OR")
			}
		}
		_, err := io.WriteString(w, e.sql)
		if err != nil {
			return nil, err
		}
		args = append(args, e.args...)
	}
	return args, nil
}

func Placeholders(count int) string {
	if count <= 0 {
		return ""
	}
	return strings.Repeat(", ?", count)[2:]
}