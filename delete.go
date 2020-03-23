package dbs

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

const (
	kDeleteBuilder = "DeleteBuilder"
)

type DeleteBuilder struct {
	builder
	prefixes statements
	options  statements
	alias    []string
	tables   statements
	using    string
	joins    statements
	wheres   statements
	orderBys []string
	limit    Statement
	offset   Statement
	suffixes statements
}

func (this *DeleteBuilder) Type() string {
	return kDeleteBuilder
}

func (this *DeleteBuilder) Prefix(sql string, args ...interface{}) *DeleteBuilder {
	this.prefixes = append(this.prefixes, NewStatement(sql, args...))
	return this
}

func (this *DeleteBuilder) Options(options ...string) *DeleteBuilder {
	for _, c := range options {
		this.options = append(this.options, NewStatement(c))
	}
	return this
}

func (this *DeleteBuilder) Alias(alias ...string) *DeleteBuilder {
	this.alias = append(this.alias, alias...)
	return this
}

func (this *DeleteBuilder) Table(table string, args ...string) *DeleteBuilder {
	var ts []string
	ts = append(ts, this.quote(table))
	ts = append(ts, args...)
	this.tables = append(this.tables, NewStatement(strings.Join(ts, " ")))
	return this
}

func (this *DeleteBuilder) USING(sql string) *DeleteBuilder {
	this.using = sql
	return this
}

func (this *DeleteBuilder) Join(join, table, suffix string, args ...interface{}) *DeleteBuilder {
	return this.join(join, table, suffix, args...)
}

func (this *DeleteBuilder) RightJoin(table, suffix string, args ...interface{}) *DeleteBuilder {
	return this.join("RIGHT JOIN", table, suffix, args...)
}

func (this *DeleteBuilder) LeftJoin(table, suffix string, args ...interface{}) *DeleteBuilder {
	return this.join("LEFT JOIN", table, suffix, args...)
}

func (this *DeleteBuilder) join(join, table, suffix string, args ...interface{}) *DeleteBuilder {
	var sql = []string{join, this.quote(table), suffix}
	this.joins = append(this.joins, NewStatement(strings.Join(sql, " "), args...))
	return this
}

func (this *DeleteBuilder) Where(sql interface{}, args ...interface{}) *DeleteBuilder {
	var stmt = parseStmt(sql, args...)
	if stmt != nil {
		this.wheres = append(this.wheres, stmt)
	}
	return this
}

func (this *DeleteBuilder) OrderBy(sql ...string) *DeleteBuilder {
	this.orderBys = append(this.orderBys, sql...)
	return this
}

func (this *DeleteBuilder) Limit(limit int64) *DeleteBuilder {
	this.limit = NewStatement(" LIMIT ?", limit)
	return this
}

func (this *DeleteBuilder) Offset(offset int64) *DeleteBuilder {
	this.offset = NewStatement(" OFFSET ?", offset)
	return this
}

func (this *DeleteBuilder) Suffix(sql interface{}, args ...interface{}) *DeleteBuilder {
	var stmt = parseStmt(sql, args...)
	if stmt != nil {
		this.suffixes = append(this.suffixes, stmt)
	}
	return this
}

func (this *DeleteBuilder) ToSQL() (string, []interface{}, error) {
	var sqlBuf = getBuffer()
	defer sqlBuf.Release()

	if err := this.WriteToSQL(sqlBuf); err != nil {
		return "", nil, err
	}

	sql, err := this.parseVal(sqlBuf.String())
	if err != nil {
		return "", nil, err
	}
	return sql, sqlBuf.Values(), nil
}

func (this *DeleteBuilder) WriteToSQL(w Writer) (err error) {
	if len(this.tables) == 0 {
		return errors.New("dbs: DELETE statement must specify a table")
	}
	if len(this.wheres) == 0 {
		return errors.New("dbs: DELETE statement must have at least one where")
	}

	if len(this.prefixes) > 0 {
		if err = this.prefixes.WriteToSQL(w, " "); err != nil {
			return err
		}
		if _, err = w.WriteString(" "); err != nil {
			return err
		}
	}

	if _, err = w.WriteString("DELETE "); err != nil {
		return err
	}

	if len(this.options) > 0 {
		if err = this.options.WriteToSQL(w, " "); err != nil {
			return err
		}
		if _, err = w.WriteString(" "); err != nil {
			return err
		}
	}

	if len(this.alias) > 0 {
		if _, err = w.WriteString(strings.Join(this.alias, ", ")); err != nil {
			return err
		}
		if _, err = w.WriteString(" "); err != nil {
			return err
		}
	}

	if _, err = w.WriteString("FROM "); err != nil {
		return err
	}

	if len(this.tables) > 0 {
		if err = this.tables.WriteToSQL(w, ", "); err != nil {
			return err
		}
	}

	if len(this.using) > 0 {
		if _, err = fmt.Fprintf(w, " USING %s", this.using); err != nil {
			return err
		}
	}

	if len(this.joins) > 0 {
		if _, err = w.WriteString(" "); err != nil {
			return err
		}
		if err = this.joins.WriteToSQL(w, " "); err != nil {
			return err
		}
	}

	if len(this.wheres) > 0 {
		if _, err = w.WriteString(" WHERE "); err != nil {
			return err
		}
		if err = this.wheres.WriteToSQL(w, " AND "); err != nil {
			return err
		}
	}

	if len(this.orderBys) > 0 {
		if _, err = w.WriteString(" ORDER BY "); err != nil {
			return err
		}
		if _, err = w.WriteString(strings.Join(this.orderBys, ", ")); err != nil {
			return err
		}
	}

	if this.limit != nil {
		if err = this.limit.WriteToSQL(w); err != nil {
			return err
		}
	}

	if this.offset != nil {
		if err = this.offset.WriteToSQL(w); err != nil {
			return err
		}
	}

	if len(this.suffixes) > 0 {
		if _, err = w.WriteString(" "); err != nil {
			return err
		}
		if err = this.suffixes.WriteToSQL(w, " "); err != nil {
			return err
		}
	}
	return nil
}

func (this *DeleteBuilder) Exec(s Session) (sql.Result, error) {
	return execContext(context.Background(), s, this)
}

func (this *DeleteBuilder) ExecContext(ctx context.Context, s Session) (result sql.Result, err error) {
	return execContext(ctx, s, this)
}

func NewDeleteBuilder() *DeleteBuilder {
	var db = &DeleteBuilder{}
	db.d = gDialect
	return db
}
