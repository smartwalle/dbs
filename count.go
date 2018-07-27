package dbs

import (
	"bytes"
	"errors"
	"io"
	"strings"
)

type CountBuilder struct {
	*SelectBuilder
	columns statements
}

func (this *CountBuilder) ToSQL() (string, []interface{}, error) {
	var sqlBuffer = &bytes.Buffer{}
	var args = newArgs()
	if err := this.AppendToSQL(sqlBuffer, args); err != nil {
		return "", nil, err
	}
	sql := sqlBuffer.String()
	log(sql, args.values)
	return sql, args.values, nil
}

func (this *CountBuilder) AppendToSQL(w io.Writer, args *Args) error {
	if len(this.columns) == 0 {
		return errors.New("SELECT statements must have at least on result column")
	}

	if len(this.prefixes) > 0 {
		if err := this.prefixes.AppendToSQL(w, " ", args); err != nil {
			return err
		}
		if _, err := io.WriteString(w, " "); err != nil {
			return err
		}
	}

	if _, err := io.WriteString(w, "SELECT "); err != nil {
		return err
	}

	if len(this.options) > 0 {
		if err := this.options.AppendToSQL(w, " ", args); err != nil {
			return err
		}
		if _, err := io.WriteString(w, " "); err != nil {
			return err
		}
	}

	if len(this.columns) > 0 {
		if err := this.columns.AppendToSQL(w, ", ", args); err != nil {
			return err
		}
	}

	if len(this.from) > 0 {
		if _, err := io.WriteString(w, " FROM "); err != nil {
			return err
		}
		if err := this.from.AppendToSQL(w, ", ", args); err != nil {
			return err
		}
	}

	if len(this.joins) > 0 {
		if _, err := io.WriteString(w, " "); err != nil {
			return err
		}
		if err := this.joins.AppendToSQL(w, " ", args); err != nil {
			return err
		}
	}

	if len(this.wheres) > 0 {
		if _, err := io.WriteString(w, " WHERE "); err != nil {
			return err
		}
		if err := this.wheres.AppendToSQL(w, " AND ", args); err != nil {
			return err
		}
	}

	if len(this.groupBys) > 0 {
		if _, err := io.WriteString(w, " GROUP BY "); err != nil {
			return err
		}
		if _, err := io.WriteString(w, strings.Join(this.groupBys, ", ")); err != nil {
			return err
		}
	}

	if len(this.havings) > 0 {
		if _, err := io.WriteString(w, " HAVING "); err != nil {
			return err
		}
		if err := this.havings.AppendToSQL(w, " AND ", args); err != nil {
			return err
		}
	}

	if len(this.orderBys) > 0 {
		if _, err := io.WriteString(w, " ORDER BY "); err != nil {
			return err
		}
		if _, err := io.WriteString(w, strings.Join(this.orderBys, ", ")); err != nil {
			return err
		}
	}

	//if this.limit != nil {
	//	if err := this.limit.AppendToSQL(w, args); err != nil {
	//		return err
	//	}
	//}
	//
	//if this.offset != nil {
	//	if err := this.offset.AppendToSQL(w, args); err != nil {
	//		return err
	//	}
	//}

	if len(this.suffixes) > 0 {
		if _, err := io.WriteString(w, " "); err != nil {
			return err
		}
		if err := this.suffixes.AppendToSQL(w, " ", args); err != nil {
			return err
		}
	}

	return nil
}

func NewCountBuilder(sb *SelectBuilder, args ...string) *CountBuilder {
	var ts []string
	ts = append(ts, "COUNT(*)")

	if len(args) > 0 {
		ts = append(ts, args...)
	}

	var cb = &CountBuilder{}
	cb.columns = append(cb.columns, NewStatement(strings.Join(ts, " ")))
	cb.SelectBuilder = sb
	cb.query = &query{sFunc: cb.ToSQL}
	cb.scan = &scan{qFunc: cb.QueryContext}
	return cb
}
