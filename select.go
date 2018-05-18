package dbs

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"strings"
)

type SelectBuilder struct {
	prefixes statements
	options  statements
	columns  statements
	from     statements
	joins    statements
	wheres   statements
	groupBys []string
	havings  statements
	orderBys []string
	limit    Statement
	offset   Statement
	suffixes statements
}

func (this *SelectBuilder) Prefix(sql string, args ...interface{}) *SelectBuilder {
	this.prefixes = append(this.prefixes, NewStatement(sql, args...))
	return this
}

func (this *SelectBuilder) Options(options ...string) *SelectBuilder {
	for _, c := range options {
		this.options = append(this.options, NewStatement(c))
	}
	return this
}

func (this *SelectBuilder) Selects(columns ...string) *SelectBuilder {
	for _, c := range columns {
		this.columns = append(this.columns, NewStatement(c))
	}
	return this
}

func (this *SelectBuilder) Select(column interface{}, args ...interface{}) *SelectBuilder {
	var stmt = parseStmt(column, args...)
	if stmt != nil {
		this.columns = append(this.columns, stmt)
	}
	return this
}

func (this *SelectBuilder) From(table string, args ...string) *SelectBuilder {
	var ts []string
	ts = append(ts, fmt.Sprintf("`%s`", table))
	ts = append(ts, args...)
	this.from = append(this.from, NewStatement(strings.Join(ts, " ")))
	return this
}

func (this *SelectBuilder) Join(join, table, suffix string, args ...interface{}) *SelectBuilder {
	return this.join(join, table, suffix, args...)
}

func (this *SelectBuilder) RightJoin(table, suffix string, args ...interface{}) *SelectBuilder {
	return this.join("RIGHT JOIN", table, suffix, args...)
}

func (this *SelectBuilder) LeftJoin(table, suffix string, args ...interface{}) *SelectBuilder {
	return this.join("LEFT JOIN", table, suffix, args...)
}

func (this *SelectBuilder) join(join, table, suffix string, args ...interface{}) *SelectBuilder {
	var sql = []string{join, fmt.Sprintf("`%s`", table), suffix}
	this.joins = append(this.joins, NewStatement(strings.Join(sql, " "), args...))
	return this
}

func (this *SelectBuilder) Where(sql interface{}, args ...interface{}) *SelectBuilder {
	var stmt = parseStmt(sql, args...)
	if stmt != nil {
		this.wheres = append(this.wheres, stmt)
	}
	return this
}

func (this *SelectBuilder) GroupBy(groupBys ...string) *SelectBuilder {
	this.groupBys = append(this.groupBys, groupBys...)
	return this
}

func (this *SelectBuilder) Having(sql string, args ...interface{}) *SelectBuilder {
	this.havings = append(this.havings, NewStatement(sql, args...))
	return this
}

func (this *SelectBuilder) OrderBy(sql ...string) *SelectBuilder {
	this.orderBys = append(this.orderBys, sql...)
	return this
}

func (this *SelectBuilder) Limit(limit uint64) *SelectBuilder {
	this.limit = NewStatement(" LIMIT ?", limit)
	return this
}

func (this *SelectBuilder) Offset(offset uint64) *SelectBuilder {
	this.offset = NewStatement(" OFFSET ?", offset)
	return this
}

func (this *SelectBuilder) Suffix(sql string, args ...interface{}) *SelectBuilder {
	this.suffixes = append(this.suffixes, NewStatement(sql, args...))
	return this
}

func (this *SelectBuilder) ToSQL() (string, []interface{}, error) {
	var sqlBuffer = &bytes.Buffer{}
	var args = newArgs()
	if err := this.AppendToSQL(sqlBuffer, args); err != nil {
		return "", nil, err
	}
	sql := sqlBuffer.String()
	log(sql, args.values)
	return sql, args.values, nil
}

func (this *SelectBuilder) AppendToSQL(w io.Writer, args *Args) error {
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

	if this.limit != nil {
		if err := this.limit.AppendToSQL(w, args); err != nil {
			return err
		}
	}

	if this.offset != nil {
		if err := this.offset.AppendToSQL(w, args); err != nil {
			return err
		}
	}

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

// --------------------------------------------------------------------------------
func (this *SelectBuilder) Query(s Executor) (*sql.Rows, error) {
	sql, args, err := this.ToSQL()
	if err != nil {
		return nil, err
	}
	return s.Query(sql, args...)
}

func (this *SelectBuilder) QueryContext(ctx context.Context, s Executor) (*sql.Rows, error) {
	sql, args, err := this.ToSQL()
	if err != nil {
		return nil, err
	}
	return s.QueryContext(ctx, sql, args...)
}

// --------------------------------------------------------------------------------
func (this *SelectBuilder) QueryTx(tx TX) (rows *sql.Rows, err error) {
	defer func() {
		if err != nil {
			tx.Rollback()
			rows = nil
		}
	}()
	rows, err = this.Query(tx)
	return rows, err
}

func (this *SelectBuilder) QueryContextTx(ctx context.Context, tx TX) (rows *sql.Rows, err error) {
	defer func() {
		if err != nil {
			tx.Rollback()
			rows = nil
		}
	}()
	rows, err = this.QueryContext(ctx, tx)
	return rows, err
}

// --------------------------------------------------------------------------------
func (this *SelectBuilder) Scan(s Executor, result interface{}) (err error) {
	rows, err := this.Query(s)
	if err != nil {
		return err
	}
	if rows != nil {
		defer rows.Close()
	}
	err = Scan(rows, result)
	return err
}

func (this *SelectBuilder) ScanContext(ctx context.Context, s Executor, result interface{}) (err error) {
	rows, err := this.QueryContext(ctx, s)
	if err != nil {
		return err
	}
	if rows != nil {
		defer rows.Close()
	}
	err = Scan(rows, result)
	return err
}

// --------------------------------------------------------------------------------
func (this *SelectBuilder) ScanTx(tx TX, result interface{}) (err error) {
	defer func() {
		if err != nil {
			tx.Rollback()
			result = nil
		}
	}()
	err = this.Scan(tx, result)
	return err
}

func (this *SelectBuilder) ScanContextTx(ctx context.Context, tx TX, result interface{}) (err error) {
	defer func() {
		if err != nil {
			tx.Rollback()
			result = nil
		}
	}()
	err = this.ScanContext(ctx, tx, result)
	return err
}

// --------------------------------------------------------------------------------
func NewSelectBuilder() *SelectBuilder {
	return &SelectBuilder{}
}
