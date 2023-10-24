package dbs

import (
	"context"
	"database/sql"
	"errors"
	"strings"
)

const (
	kDeleteBuilder = "DeleteBuilder"
)

type DeleteBuilder struct {
	builder
	prefixes Clauses
	options  Clauses
	alias    []string
	tables   Clauses
	using    string
	joins    Clauses
	wheres   Clauses
	orderBys []string
	limit    SQLClause
	offset   SQLClause
	suffixes Clauses
}

func (db *DeleteBuilder) Type() string {
	return kDeleteBuilder
}

func (db *DeleteBuilder) UsePlaceholder(p Placeholder) *DeleteBuilder {
	db.builder.UsePlaceholder(p)
	return db
}

func (db *DeleteBuilder) Prefix(sql string, args ...interface{}) *DeleteBuilder {
	db.prefixes = append(db.prefixes, NewClause(sql, args...))
	return db
}

func (db *DeleteBuilder) Options(options ...string) *DeleteBuilder {
	for _, opt := range options {
		db.options = append(db.options, NewClause(opt))
	}
	return db
}

func (db *DeleteBuilder) Alias(alias ...string) *DeleteBuilder {
	db.alias = append(db.alias, alias...)
	return db
}

func (db *DeleteBuilder) Table(table string, args ...string) *DeleteBuilder {
	var ts []string
	ts = append(ts, db.quote(table))
	ts = append(ts, args...)
	db.tables = append(db.tables, NewClause(strings.Join(ts, " ")))
	return db
}

func (db *DeleteBuilder) USING(sql string) *DeleteBuilder {
	db.using = sql
	return db
}

func (db *DeleteBuilder) Join(join, table, suffix string, args ...interface{}) *DeleteBuilder {
	return db.join(join, table, suffix, args...)
}

func (db *DeleteBuilder) RightJoin(table, suffix string, args ...interface{}) *DeleteBuilder {
	return db.join("RIGHT JOIN", table, suffix, args...)
}

func (db *DeleteBuilder) LeftJoin(table, suffix string, args ...interface{}) *DeleteBuilder {
	return db.join("LEFT JOIN", table, suffix, args...)
}

func (db *DeleteBuilder) join(join, table, suffix string, args ...interface{}) *DeleteBuilder {
	var nSQL = []string{join, db.quote(table), suffix}
	db.joins = append(db.joins, NewClause(strings.Join(nSQL, " "), args...))
	return db
}

func (db *DeleteBuilder) Where(sql interface{}, args ...interface{}) *DeleteBuilder {
	var clause = parseClause(sql, args...)
	if clause != nil {
		db.wheres = append(db.wheres, clause)
	}
	return db
}

func (db *DeleteBuilder) OrderBy(sql ...string) *DeleteBuilder {
	db.orderBys = append(db.orderBys, sql...)
	return db
}

func (db *DeleteBuilder) Limit(limit int64) *DeleteBuilder {
	db.limit = NewClause(" LIMIT ?", limit)
	return db
}

func (db *DeleteBuilder) Offset(offset int64) *DeleteBuilder {
	db.offset = NewClause(" OFFSET ?", offset)
	return db
}

func (db *DeleteBuilder) Suffix(sql interface{}, args ...interface{}) *DeleteBuilder {
	var clause = parseClause(sql, args...)
	if clause != nil {
		db.suffixes = append(db.suffixes, clause)
	}
	return db
}

func (db *DeleteBuilder) SQL() (string, []interface{}, error) {
	var sqlBuf = getBuffer()
	defer sqlBuf.Release()

	if err := db.Write(sqlBuf); err != nil {
		return "", nil, err
	}

	nSQL, err := db.replace(sqlBuf.String())
	if err != nil {
		return "", nil, err
	}
	return nSQL, sqlBuf.Values(), nil
}

func (db *DeleteBuilder) Write(w Writer) (err error) {
	if len(db.tables) == 0 {
		return errors.New("dbs: DELETE clause must specify a table")
	}
	if len(db.wheres) == 0 {
		return errors.New("dbs: DELETE clause must have at least one where")
	}

	if len(db.prefixes) > 0 {
		if err = db.prefixes.Write(w, " "); err != nil {
			return err
		}
		if _, err = w.WriteString(" "); err != nil {
			return err
		}
	}

	if _, err = w.WriteString("DELETE "); err != nil {
		return err
	}

	if len(db.options) > 0 {
		if err = db.options.Write(w, " "); err != nil {
			return err
		}
		if _, err = w.WriteString(" "); err != nil {
			return err
		}
	}

	if len(db.alias) > 0 {
		if _, err = w.WriteString(strings.Join(db.alias, ", ")); err != nil {
			return err
		}
		if _, err = w.WriteString(" "); err != nil {
			return err
		}
	}

	if _, err = w.WriteString("FROM "); err != nil {
		return err
	}

	if len(db.tables) > 0 {
		if err = db.tables.Write(w, ", "); err != nil {
			return err
		}
	}

	if len(db.using) > 0 {
		if _, err = w.WriteString(" USING "); err != nil {
			return err
		}
		if _, err = w.WriteString(db.using); err != nil {
			return err
		}
	}

	if len(db.joins) > 0 {
		if _, err = w.WriteString(" "); err != nil {
			return err
		}
		if err = db.joins.Write(w, " "); err != nil {
			return err
		}
	}

	if len(db.wheres) > 0 {
		if _, err = w.WriteString(" WHERE "); err != nil {
			return err
		}
		if err = db.wheres.Write(w, " AND "); err != nil {
			return err
		}
	}

	if len(db.orderBys) > 0 {
		if _, err = w.WriteString(" ORDER BY "); err != nil {
			return err
		}
		if _, err = w.WriteString(strings.Join(db.orderBys, ", ")); err != nil {
			return err
		}
	}

	if db.limit != nil {
		if err = db.limit.Write(w); err != nil {
			return err
		}
	}

	if db.offset != nil {
		if err = db.offset.Write(w); err != nil {
			return err
		}
	}

	if len(db.suffixes) > 0 {
		if _, err = w.WriteString(" "); err != nil {
			return err
		}
		if err = db.suffixes.Write(w, " "); err != nil {
			return err
		}
	}
	return nil
}

func (db *DeleteBuilder) Exec(s Session) (sql.Result, error) {
	return execContext(context.Background(), s, db)
}

func (db *DeleteBuilder) ExecContext(ctx context.Context, s Session) (result sql.Result, err error) {
	return execContext(ctx, s, db)
}

func NewDeleteBuilder() *DeleteBuilder {
	var db = &DeleteBuilder{}
	db.placeholder = gPlaceholder
	return db
}
