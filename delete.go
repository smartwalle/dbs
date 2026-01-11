package dbs

import (
	"context"
	"database/sql"
	"errors"
)

type DeleteBuilder struct {
	dialect  Dialect
	session  Session
	prefixes *Clauses
	options  *Clauses
	table    string
	wheres   *Conds
	orderBys *Clauses
	limit    SQLClause
	suffixes *Clauses
}

func NewDeleteBuilder() *DeleteBuilder {
	var db = &DeleteBuilder{}
	db.dialect = GetDialect()
	return db
}

func (db *DeleteBuilder) Reset() {
	db.dialect = nil
	db.session = nil
	db.prefixes.reset()
	db.options.reset()
	db.table = ""
	db.wheres.reset()
	db.orderBys.reset()
	db.limit = nil
	db.suffixes.reset()
}

func (db *DeleteBuilder) UseDialect(dialect Dialect) *DeleteBuilder {
	db.dialect = dialect
	return db
}

func (db *DeleteBuilder) UseSession(session Session) *DeleteBuilder {
	db.session = session
	return db
}

func (db *DeleteBuilder) Prefix(sql interface{}, args ...interface{}) *DeleteBuilder {
	if db.prefixes == nil {
		db.prefixes = NewClauses(' ')
	}
	db.prefixes.Append(sql, args...)
	return db
}

func (db *DeleteBuilder) Option(sql interface{}, args ...interface{}) *DeleteBuilder {
	if db.options == nil {
		db.options = NewClauses(' ')
	}
	db.options.Append(sql, args...)
	return db
}

func (db *DeleteBuilder) Table(table string) *DeleteBuilder {
	db.table = table
	return db
}

func (db *DeleteBuilder) Where(sql interface{}, args ...interface{}) *DeleteBuilder {
	if db.wheres == nil {
		var conds = AND()
		conds.ignoreBracket = true
		db.wheres = conds
	}
	db.wheres.Append(sql, args...)
	return db
}

func (db *DeleteBuilder) OrderBy(sql interface{}, args ...interface{}) *DeleteBuilder {
	if db.orderBys == nil {
		db.orderBys = NewClauses(',')
	}
	db.orderBys.Append(sql, args...)
	return db
}

func (db *DeleteBuilder) Limit(limit int64) *DeleteBuilder {
	db.limit = NewClause(" LIMIT ?", limit)
	return db
}

func (db *DeleteBuilder) Suffix(sql interface{}, args ...interface{}) *DeleteBuilder {
	if db.suffixes == nil {
		db.suffixes = NewClauses(' ')
	}
	db.suffixes.Append(sql, args...)
	return db
}

func (db *DeleteBuilder) Write(w Writer) (err error) {
	if len(db.table) == 0 {
		return errors.New("dbs: delete clause must specify a table")
	}
	if !db.wheres.valid() {
		return errors.New("dbs: delete clause must have at least one where")
	}

	if db.prefixes.valid() {
		if err = db.prefixes.Write(w); err != nil {
			return err
		}
		if err = w.WriteByte(' '); err != nil {
			return err
		}
	}

	if _, err = w.WriteString("DELETE "); err != nil {
		return err
	}

	if db.options.valid() {
		if err = db.options.Write(w); err != nil {
			return err
		}
		if err = w.WriteByte(' '); err != nil {
			return err
		}
	}

	if _, err = w.WriteString("FROM "); err != nil {
		return err
	}
	if _, err = w.WriteString(db.table); err != nil {
		return err
	}

	if db.wheres.valid() {
		if _, err = w.WriteString(" WHERE "); err != nil {
			return err
		}
		if err = db.wheres.Write(w); err != nil {
			return err
		}
	}

	if db.orderBys.valid() {
		if _, err = w.WriteString(" ORDER BY "); err != nil {
			return err
		}
		if err = db.orderBys.Write(w); err != nil {
			return err
		}
	}

	if db.limit != nil {
		if err = db.limit.Write(w); err != nil {
			return err
		}
	}

	if db.suffixes.valid() {
		if err = w.WriteByte(' '); err != nil {
			return err
		}
		if err = db.suffixes.Write(w); err != nil {
			return err
		}
	}

	return nil
}

func (db *DeleteBuilder) SQL() (string, []interface{}, error) {
	var buffer = NewBuffer()
	defer buffer.Release()

	buffer.UseDialect(db.dialect)

	if err := db.Write(buffer); err != nil {
		return "", nil, err
	}
	return buffer.String(), buffer.Arguments(), nil
}

func (db *DeleteBuilder) Scan(ctx context.Context, dest interface{}) error {
	return scan(ctx, db.session, db, dest)
}

func (db *DeleteBuilder) Exec(ctx context.Context) (sql.Result, error) {
	return exec(ctx, db.session, db)
}
