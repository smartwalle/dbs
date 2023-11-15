package dbs

import (
	"context"
	"database/sql"
	"errors"
	"strings"
)

const (
	kUpdateBuilder = "UpdateBuilder"
)

type UpdateBuilder struct {
	builder
	prefixes Clauses
	options  Clauses
	tables   Clauses
	joins    Clauses
	columns  setClauses
	wheres   Clauses
	orderBys []string
	limit    SQLClause
	offset   SQLClause
	suffixes Clauses
}

func (ub *UpdateBuilder) Type() string {
	return kUpdateBuilder
}

func (ub *UpdateBuilder) UsePlaceholder(p Placeholder) *UpdateBuilder {
	ub.builder.UsePlaceholder(p)
	return ub
}

func (ub *UpdateBuilder) Prefix(clause string, args ...interface{}) *UpdateBuilder {
	ub.prefixes = append(ub.prefixes, NewClause(clause, args...))
	return ub
}

func (ub *UpdateBuilder) Options(options ...string) *UpdateBuilder {
	for _, opt := range options {
		ub.options = append(ub.options, NewClause(opt))
	}
	return ub
}

func (ub *UpdateBuilder) Table(table string, args ...string) *UpdateBuilder {
	var ts []string
	ts = append(ts, ub.quote(table))
	ts = append(ts, args...)
	ub.tables = append(ub.tables, NewClause(strings.Join(ts, " ")))
	return ub
}

func (ub *UpdateBuilder) Join(join, table, suffix string, args ...interface{}) *UpdateBuilder {
	return ub.join(join, table, suffix, args...)
}

func (ub *UpdateBuilder) RightJoin(table, suffix string, args ...interface{}) *UpdateBuilder {
	return ub.join("RIGHT JOIN", table, suffix, args...)
}

func (ub *UpdateBuilder) LeftJoin(table, suffix string, args ...interface{}) *UpdateBuilder {
	return ub.join("LEFT JOIN", table, suffix, args...)
}

func (ub *UpdateBuilder) join(join, table, suffix string, args ...interface{}) *UpdateBuilder {
	var nClause = []string{join, ub.quote(table), suffix}
	ub.joins = append(ub.joins, NewClause(strings.Join(nClause, " "), args...))
	return ub
}

func (ub *UpdateBuilder) SET(column string, value interface{}) *UpdateBuilder {
	ub.columns = append(ub.columns, newSet(ub.quote(column), value))
	return ub
}

// SETS 批量设置需要更新的字段及其值
//
//	var name = "my name"
//	SETS("name", name, "age", 10)
func (ub *UpdateBuilder) SETS(kvs ...interface{}) *UpdateBuilder {
	var column string
	for i, value := range kvs {
		if i%2 == 0 {
			column = value.(string)
			continue
		}
		ub.SET(column, value)
	}
	return ub
}

func (ub *UpdateBuilder) SetMap(data map[string]interface{}) *UpdateBuilder {
	for k, v := range data {
		ub.SET(k, v)
	}
	return ub
}

func (ub *UpdateBuilder) Where(clause interface{}, args ...interface{}) *UpdateBuilder {
	var nClause = parseClause(clause, args...)
	if nClause != nil {
		ub.wheres = append(ub.wheres, nClause)
	}
	return ub
}

func (ub *UpdateBuilder) OrderBy(clause ...string) *UpdateBuilder {
	ub.orderBys = append(ub.orderBys, clause...)
	return ub
}

func (ub *UpdateBuilder) Limit(limit int64) *UpdateBuilder {
	ub.limit = NewClause(" LIMIT ?", limit)
	return ub
}

func (ub *UpdateBuilder) Offset(offset int64) *UpdateBuilder {
	ub.offset = NewClause(" OFFSET ?", offset)
	return ub
}

func (ub *UpdateBuilder) Suffix(clause interface{}, args ...interface{}) *UpdateBuilder {
	var nClause = parseClause(clause, args...)
	if nClause != nil {
		ub.suffixes = append(ub.suffixes, nClause)
	}
	return ub
}

func (ub *UpdateBuilder) SQL() (string, []interface{}, error) {
	var buf = getBuffer()
	defer buf.Release()

	if err := ub.Write(buf); err != nil {
		return "", nil, err
	}

	clause, err := ub.replace(buf.String())
	if err != nil {
		return "", nil, err
	}
	return clause, buf.Values(), nil
}

func (ub *UpdateBuilder) Write(w Writer) (err error) {
	if len(ub.tables) == 0 {
		return errors.New("dbs: Update clause must specify a table")
	}
	if len(ub.columns) == 0 {
		return errors.New("dbs: Update clause must have at least one Set")
	}
	if len(ub.wheres) == 0 {
		return errors.New("dbs: Update clause must have at least one where")
	}

	if len(ub.prefixes) > 0 {
		if err = ub.prefixes.Write(w, " "); err != nil {
			return err
		}
		if _, err = w.WriteString(" "); err != nil {
			return err
		}
	}

	if _, err = w.WriteString("UPDATE "); err != nil {
		return err
	}

	if len(ub.options) > 0 {
		if err = ub.options.Write(w, " "); err != nil {
			return err
		}
		if _, err = w.WriteString(" "); err != nil {
			return err
		}
	}

	if len(ub.tables) > 0 {
		if err = ub.tables.Write(w, ", "); err != nil {
			return err
		}
	}

	if len(ub.joins) > 0 {
		if _, err = w.WriteString(" "); err != nil {
			return err
		}
		if err = ub.joins.Write(w, " "); err != nil {
			return err
		}
	}

	if _, err = w.WriteString(" SET "); err != nil {
		return err
	}

	if len(ub.columns) > 0 {
		if err = ub.columns.Write(w, ", "); err != nil {
			return err
		}
	}

	if len(ub.wheres) > 0 {
		if _, err = w.WriteString(" WHERE "); err != nil {
			return err
		}
		if err = ub.wheres.Write(w, " AND "); err != nil {
			return err
		}
	}

	if len(ub.orderBys) > 0 {
		if _, err = w.WriteString(" ORDER BY "); err != nil {
			return err
		}
		if _, err = w.WriteString(strings.Join(ub.orderBys, ", ")); err != nil {
			return err
		}
	}

	if ub.limit != nil {
		if err = ub.limit.Write(w); err != nil {
			return err
		}
	}

	if ub.offset != nil {
		if err = ub.offset.Write(w); err != nil {
			return err
		}
	}

	if len(ub.suffixes) > 0 {
		if _, err = w.WriteString(" "); err != nil {
			return err
		}
		if err = ub.suffixes.Write(w, " "); err != nil {
			return err
		}
	}

	return nil
}

func (ub *UpdateBuilder) Exec(s Session) (sql.Result, error) {
	return exec(context.Background(), s, ub)
}

func (ub *UpdateBuilder) ExecContext(ctx context.Context, s Session) (result sql.Result, err error) {
	return exec(ctx, s, ub)
}

func (ub *UpdateBuilder) Scan(s Session, dst interface{}) (err error) {
	return scan(context.Background(), s, ub, dst)
}

func (ub *UpdateBuilder) ScanContext(ctx context.Context, s Session, dst interface{}) (err error) {
	return scan(ctx, s, ub, dst)
}

func (ub *UpdateBuilder) ScanRow(s Session, dst ...interface{}) (err error) {
	return scanRow(context.Background(), s, ub, dst...)
}

func (ub *UpdateBuilder) ScanRowContext(ctx context.Context, s Session, dst ...interface{}) (err error) {
	return scanRow(ctx, s, ub, dst...)
}

func (ub *UpdateBuilder) Query(s Session) (*sql.Rows, error) {
	return query(context.Background(), s, ub)
}

func (ub *UpdateBuilder) QueryContext(ctx context.Context, s Session) (*sql.Rows, error) {
	return query(ctx, s, ub)
}

func NewUpdateBuilder() *UpdateBuilder {
	var ub = &UpdateBuilder{}
	ub.placeholder = gPlaceholder
	return ub
}

func Update(table string, args ...string) *UpdateBuilder {
	var ub = NewUpdateBuilder()
	ub.Table(table, args...)
	return ub
}
