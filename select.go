package dbs

import (
	"context"
	"database/sql"
	"errors"
	"strings"
)

const (
	kCount = "COUNT(1)"
)

const (
	kSelectBuilder = "SelectBuilder"
)

type SelectBuilder struct {
	builder
	prefixes Clauses
	options  Clauses
	columns  []string
	selects  Clauses
	from     Clauses
	joins    Clauses
	wheres   Clauses
	groupBys []string
	having   Clauses
	orderBys []string
	limit    SQLClause
	offset   SQLClause
	suffixes Clauses
}

func (sb *SelectBuilder) Type() string {
	return kSelectBuilder
}

func (sb *SelectBuilder) UsePlaceholder(p Placeholder) *SelectBuilder {
	sb.builder.UsePlaceholder(p)
	return sb
}

func (sb *SelectBuilder) Prefix(sql string, args ...interface{}) *SelectBuilder {
	sb.prefixes = append(sb.prefixes, NewClause(sql, args...))
	return sb
}

func (sb *SelectBuilder) Options(options ...string) *SelectBuilder {
	for _, opt := range options {
		sb.options = append(sb.options, NewClause(opt))
	}
	return sb
}

func (sb *SelectBuilder) Selects(columns ...string) *SelectBuilder {
	sb.columns = append(sb.columns, columns...)
	return sb
}

func (sb *SelectBuilder) Select(column interface{}, args ...interface{}) *SelectBuilder {
	var clause = parseClause(column, args...)

	if clause != nil {
		sb.selects = append(sb.selects, clause)
	}
	return sb
}

func (sb *SelectBuilder) From(table string, args ...string) *SelectBuilder {
	var argsLen = len(args)
	var ts = make([]string, 0, 1+argsLen)
	ts = append(ts, sb.quote(table))
	if argsLen > 0 {
		ts = append(ts, args...)
	}
	sb.from = append(sb.from, NewClause(strings.Join(ts, " ")))
	return sb
}

func (sb *SelectBuilder) FromClause(clause SQLClause) *SelectBuilder {
	sb.from = append(sb.from, clause)
	return sb
}

func (sb *SelectBuilder) Join(join, table, suffix string, args ...interface{}) *SelectBuilder {
	return sb.join(join, table, suffix, args...)
}

func (sb *SelectBuilder) RightJoin(table, suffix string, args ...interface{}) *SelectBuilder {
	return sb.join("RIGHT JOIN", table, suffix, args...)
}

func (sb *SelectBuilder) LeftJoin(table, suffix string, args ...interface{}) *SelectBuilder {
	return sb.join("LEFT JOIN", table, suffix, args...)
}

func (sb *SelectBuilder) join(join, table, suffix string, args ...interface{}) *SelectBuilder {
	var nSQL = []string{join, sb.quote(table), suffix}
	sb.joins = append(sb.joins, NewClause(strings.Join(nSQL, " "), args...))
	return sb
}

func (sb *SelectBuilder) Where(sql interface{}, args ...interface{}) *SelectBuilder {
	var clause = parseClause(sql, args...)
	if clause != nil {
		sb.wheres = append(sb.wheres, clause)
	}
	return sb
}

func (sb *SelectBuilder) GroupBy(groupBys ...string) *SelectBuilder {
	sb.groupBys = append(sb.groupBys, groupBys...)
	return sb
}

func (sb *SelectBuilder) Having(sql interface{}, args ...interface{}) *SelectBuilder {
	var clause = parseClause(sql, args...)
	if clause != nil {
		sb.having = append(sb.having, clause)
	}
	return sb
}

func (sb *SelectBuilder) OrderBy(sql ...string) *SelectBuilder {
	sb.orderBys = append(sb.orderBys, sql...)
	return sb
}

func (sb *SelectBuilder) Limit(limit int64) *SelectBuilder {
	sb.limit = NewClause(" LIMIT ?", limit)
	return sb
}

func (sb *SelectBuilder) Offset(offset int64) *SelectBuilder {
	sb.offset = NewClause(" OFFSET ?", offset)
	return sb
}

func (sb *SelectBuilder) Suffix(sql interface{}, args ...interface{}) *SelectBuilder {
	var clause = parseClause(sql, args...)
	if clause != nil {
		sb.suffixes = append(sb.suffixes, clause)
	}
	return sb
}

func (sb *SelectBuilder) SQL() (string, []interface{}, error) {
	var sqlBuf = getBuffer()
	defer sqlBuf.Release()

	if err := sb.Write(sqlBuf); err != nil {
		return "", nil, err
	}

	nSQL, err := sb.replace(sqlBuf.String())
	if err != nil {
		return "", nil, err
	}
	return nSQL, sqlBuf.Values(), nil
}

func (sb *SelectBuilder) Write(w Writer) (err error) {
	if len(sb.columns) == 0 && len(sb.selects) == 0 {
		return errors.New("dbs: SELECT clause must have at least one result column")
	}

	if len(sb.prefixes) > 0 {
		if err = sb.prefixes.Write(w, " "); err != nil {
			return err
		}
		if _, err = w.WriteString(" "); err != nil {
			return err
		}
	}

	if _, err = w.WriteString("SELECT "); err != nil {
		return err
	}

	if len(sb.options) > 0 {
		if err = sb.options.Write(w, " "); err != nil {
			return err
		}
		if _, err = w.WriteString(" "); err != nil {
			return err
		}
	}

	if len(sb.columns) > 0 {
		if _, err = w.WriteString(strings.Join(sb.columns, ", ")); err != nil {
			return err
		}
	}

	if len(sb.selects) > 0 {
		if len(sb.columns) > 0 {
			if _, err = w.WriteString(", "); err != nil {
				return err
			}
		}
		if err = sb.selects.Write(w, ", "); err != nil {
			return err
		}
	}

	if len(sb.from) > 0 {
		if _, err = w.WriteString(" FROM "); err != nil {
			return err
		}
		if err = sb.from.Write(w, ", "); err != nil {
			return err
		}
	}

	if len(sb.joins) > 0 {
		if _, err = w.WriteString(" "); err != nil {
			return err
		}
		if err = sb.joins.Write(w, " "); err != nil {
			return err
		}
	}

	if len(sb.wheres) > 0 {
		if _, err = w.WriteString(" WHERE "); err != nil {
			return err
		}
		if err = sb.wheres.Write(w, " AND "); err != nil {
			return err
		}
	}

	if len(sb.groupBys) > 0 {
		if _, err = w.WriteString(" GROUP BY "); err != nil {
			return err
		}
		if _, err = w.WriteString(strings.Join(sb.groupBys, ", ")); err != nil {
			return err
		}
	}

	if len(sb.having) > 0 {
		if _, err = w.WriteString(" HAVING "); err != nil {
			return err
		}
		if err = sb.having.Write(w, " AND "); err != nil {
			return err
		}
	}

	if len(sb.orderBys) > 0 {
		if _, err = w.WriteString(" ORDER BY "); err != nil {
			return err
		}
		if _, err = w.WriteString(strings.Join(sb.orderBys, ", ")); err != nil {
			return err
		}
	}

	if sb.limit != nil {
		if err = sb.limit.Write(w); err != nil {
			return err
		}
	}

	if sb.offset != nil {
		if err = sb.offset.Write(w); err != nil {
			return err
		}
	}

	if len(sb.suffixes) > 0 {
		if _, err = w.WriteString(" "); err != nil {
			return err
		}
		if err = sb.suffixes.Write(w, " "); err != nil {
			return err
		}
	}

	return nil
}

func (sb *SelectBuilder) Count(alias string) *SelectBuilder {
	var columns = []string{strings.Join([]string{kCount, alias}, " ")}

	var temp = *sb
	var count = &temp
	count.columns = columns
	count.selects = nil
	count.limit = nil
	count.offset = nil
	count.orderBys = nil

	var nSelect *SelectBuilder
	if len(count.groupBys) > 0 {
		nSelect = NewSelectBuilder()
		nSelect.FromClause(Alias(count, "c"))
		nSelect.columns = columns
	} else {
		nSelect = count
	}

	return nSelect
}

// Scan 读取数据到一个结构体中。
//
// var user *User
//
// var sb = dbs.NewSelectBuilder()
//
// sb.Scan(db, &user)
func (sb *SelectBuilder) Scan(s Session, dst interface{}) (err error) {
	return scanContext(context.Background(), s, sb, dst)
}

// ScanContext 读取数据到一个结构体中。
//
// var user *User
//
// var sb = dbs.NewSelectBuilder()
//
// sb.ScanContext(ctx, db, &user)
func (sb *SelectBuilder) ScanContext(ctx context.Context, s Session, dst interface{}) (err error) {
	return scanContext(ctx, s, sb, dst)
}

// ScanRow 读取数据到基本数据类型的变量中，类似于 database/sql 包中结构体 Rows 的 Scan() 方法。
//
// var name string
//
// var age int
//
// var sb = dbs.NewSelectBuilder()
//
// sb.ScanRow(db, &name, &age)
func (sb *SelectBuilder) ScanRow(s Session, dst ...interface{}) (err error) {
	return scanRowContext(context.Background(), s, sb, dst...)
}

// ScanRowContext 读取数据到基本数据类型的变量中，类似于 database/sql 包中结构体 Rows 的 Scan() 方法。
//
// var name string
//
// var age int
//
// var sb = dbs.NewSelectBuilder()
//
// sb.ScanRowContext(ctx, db, &name, &age)
func (sb *SelectBuilder) ScanRowContext(ctx context.Context, s Session, dst ...interface{}) (err error) {
	return scanRowContext(ctx, s, sb, dst...)
}

func (sb *SelectBuilder) Query(s Session) (*sql.Rows, error) {
	return queryContext(context.Background(), s, sb)
}

func (sb *SelectBuilder) QueryContext(ctx context.Context, s Session) (*sql.Rows, error) {
	return queryContext(ctx, s, sb)
}

func NewSelectBuilder() *SelectBuilder {
	var sb = &SelectBuilder{}
	sb.placeholder = gPlaceholder
	return sb
}

func Select(columns ...string) *SelectBuilder {
	var sb = NewSelectBuilder()
	sb.Selects(columns...)
	return sb
}
