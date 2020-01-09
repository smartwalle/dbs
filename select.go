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
	d        dialect
	prefixes statements
	options  statements
	columns  statements
	from     statements
	joins    statements
	wheres   statements
	groupBys []string
	having   statements
	orderBys []string
	limit    Statement
	offset   Statement
	suffixes statements
}

func (this *SelectBuilder) Type() string {
	return kSelectBuilder
}

func (this *SelectBuilder) Clone() *SelectBuilder {
	var sb = *this
	return &sb
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
	var argsLen = len(args)
	var ts = make([]string, 0, 1+argsLen)
	ts = append(ts, this.quote(table))
	if argsLen > 0 {
		ts = append(ts, args...)
	}
	this.from = append(this.from, NewStatement(strings.Join(ts, " ")))
	return this
}

func (this *SelectBuilder) FromStmt(stmt Statement) *SelectBuilder {
	this.from = append(this.from, stmt)
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
	var sql = []string{join, this.quote(table), suffix}
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

func (this *SelectBuilder) Having(sql interface{}, args ...interface{}) *SelectBuilder {
	var stmt = parseStmt(sql, args...)
	if stmt != nil {
		this.having = append(this.having, stmt)
	}
	return this
}

func (this *SelectBuilder) OrderBy(sql ...string) *SelectBuilder {
	this.orderBys = append(this.orderBys, sql...)
	return this
}

func (this *SelectBuilder) Limit(limit int64) *SelectBuilder {
	this.limit = NewStatement(" LIMIT ?", limit)
	return this
}

func (this *SelectBuilder) Offset(offset int64) *SelectBuilder {
	this.offset = NewStatement(" OFFSET ?", offset)
	return this
}

func (this *SelectBuilder) Suffix(sql interface{}, args ...interface{}) *SelectBuilder {
	var stmt = parseStmt(sql, args...)
	if stmt != nil {
		this.suffixes = append(this.suffixes, stmt)
	}
	return this
}

func (this *SelectBuilder) ToSQL() (string, []interface{}, error) {
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

func (this *SelectBuilder) WriteToSQL(w Writer) (err error) {
	if len(this.columns) == 0 {
		return errors.New("dbs: SELECT statement must have at least one result column")
	}

	if len(this.prefixes) > 0 {
		if err = this.prefixes.WriteToSQL(w, " "); err != nil {
			return err
		}
		if _, err = w.WriteString(" "); err != nil {
			return err
		}
	}

	if _, err = w.WriteString("SELECT "); err != nil {
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

	if len(this.columns) > 0 {
		if err = this.columns.WriteToSQL(w, ", "); err != nil {
			return err
		}
	}

	if len(this.from) > 0 {
		if _, err = w.WriteString(" FROM "); err != nil {
			return err
		}
		if err = this.from.WriteToSQL(w, ", "); err != nil {
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

	if len(this.groupBys) > 0 {
		if _, err = w.WriteString(" GROUP BY "); err != nil {
			return err
		}
		if _, err = w.WriteString(strings.Join(this.groupBys, ", ")); err != nil {
			return err
		}
	}

	if len(this.having) > 0 {
		if _, err = w.WriteString(" HAVING "); err != nil {
			return err
		}
		if err = this.having.WriteToSQL(w, " AND "); err != nil {
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

func (this *SelectBuilder) Count(args ...string) *SelectBuilder {
	var ts = []string{kCount}

	if len(args) > 0 {
		ts = append(ts, args...)
	}

	var sb = NewSelectBuilder()
	var cb = this.Clone()
	cb.columns = statements{NewStatement(strings.Join(ts, " "))}
	cb.limit = nil
	cb.offset = nil
	cb.orderBys = nil

	if len(cb.groupBys) > 0 {
		sb.FromStmt(Alias(cb, "c"))
		sb.columns = statements{NewStatement(strings.Join(ts, " "))}
	} else {
		sb = cb
	}

	return sb
}

func (this *SelectBuilder) UseDialect(d dialect) {
	this.d = d
}

func (this *SelectBuilder) quote(s string) string {
	if strings.Index(s, ".") != -1 {
		var newStrs []string
		for _, s := range strings.Split(s, ".") {
			newStrs = append(newStrs, this.d.Quote(s))
		}
		return strings.Join(newStrs, ".")
	}
	return this.d.Quote(s)
}

func (this *SelectBuilder) parseVal(sql string) (string, error) {
	return this.d.ParseVal(sql)
}

// Scan 读取数据到一个结构体中。
// 需要注意：声明变量的时候，变量应该为某一结构体的指针类型，不需要初始化，调用 Scan() 方法的时候，需要传递变量的地址。
// var user *User
//
// var sb = dbs.NewSelectBuilder()
// sb.Scan(db, &user)
func (this *SelectBuilder) Scan(s Session, dest interface{}) (err error) {
	return scanContext(context.Background(), s, this, dest)
}

// ScanContext 读取数据到一个结构体中。
// 需要注意：声明变量的时候，变量应该为某一结构体的指针类型，不需要初始化，调用 ScanContext() 方法的时候，需要传递变量的地址。
// var user *User
//
// var sb = dbs.NewSelectBuilder()
// sb.ScanContext(ctx, db, &user)
func (this *SelectBuilder) ScanContext(ctx context.Context, s Session, dest interface{}) (err error) {
	return scanContext(ctx, s, this, dest)
}

// ScanRow 读取数据到基本数据类型的变量中，类似于 database/sql 包中结构体 Rows 的 Scan() 方法。
// var name string
// var age int
//
// var sb = dbs.NewSelectBuilder()
// sb.ScanRow(db, &name, &age)
func (this *SelectBuilder) ScanRow(s Session, dest ...interface{}) (err error) {
	return scanRowContext(context.Background(), s, this, dest...)
}

// ScanRowContext 读取数据到基本数据类型的变量中，类似于 database/sql 包中结构体 Rows 的 Scan() 方法。
// var name string
// var age int
//
// var sb = dbs.NewSelectBuilder()
// sb.ScanRowContext(ctx, db, &name, &age)
func (this *SelectBuilder) ScanRowContext(ctx context.Context, s Session, dest ...interface{}) (err error) {
	return scanRowContext(ctx, s, this, dest...)
}

func (this *SelectBuilder) Query(s Session) (*sql.Rows, error) {
	return queryContext(context.Background(), s, this)
}

func (this *SelectBuilder) QueryContext(ctx context.Context, s Session) (*sql.Rows, error) {
	return queryContext(ctx, s, this)
}

func NewSelectBuilder() *SelectBuilder {
	var sb = &SelectBuilder{}
	sb.d = gDialect
	return sb
}

func Select(columns ...string) *SelectBuilder {
	var sb = NewSelectBuilder()
	sb.Selects(columns...)
	return sb
}
