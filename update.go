package dbs

import (
	"context"
	"database/sql"
	"errors"
)

type UpdateBuilder struct {
	placeholder Placeholder
	session     Session
	prefixes    *Clauses
	options     *Clauses
	table       string
	sets        []Set
	wheres      *Conds
	orderBys    *Clauses
	limit       SQLClause
	suffixes    *Clauses
}

func NewUpdateBuilder() *UpdateBuilder {
	var sb = &UpdateBuilder{}
	sb.placeholder = GlobalPlaceholder()
	return sb
}

func (ub *UpdateBuilder) UsePlaceholder(p Placeholder) *UpdateBuilder {
	ub.placeholder = p
	return ub
}

func (ub *UpdateBuilder) UseSession(s Session) *UpdateBuilder {
	ub.session = s
	return ub
}

func (ub *UpdateBuilder) Prefix(sql interface{}, args ...interface{}) *UpdateBuilder {
	if ub.prefixes == nil {
		ub.prefixes = NewClauses(' ')
	}
	ub.prefixes.Append(sql, args...)
	return ub
}

func (ub *UpdateBuilder) Option(sql interface{}, args ...interface{}) *UpdateBuilder {
	if ub.options == nil {
		ub.options = NewClauses(' ')
	}
	ub.options.Append(sql, args...)
	return ub
}

func (ub *UpdateBuilder) Table(table string) *UpdateBuilder {
	ub.table = table
	return ub
}

func (ub *UpdateBuilder) Set(column string, value interface{}) *UpdateBuilder {
	ub.sets = append(ub.sets, NewSet(column, value))
	return ub
}

func (ub *UpdateBuilder) SetValues(values map[string]interface{}) *UpdateBuilder {
	var sets = make([]Set, 0, len(values))
	for key, value := range values {
		sets = append(sets, NewSet(key, value))
	}
	ub.sets = append(ub.sets, sets...)
	return ub
}

func (ub *UpdateBuilder) Where(sql interface{}, args ...interface{}) *UpdateBuilder {
	if ub.wheres == nil {
		var conds = AND()
		conds.ignoreBracket = true
		ub.wheres = conds
	}
	ub.wheres.Append(sql, args...)
	return ub
}

func (ub *UpdateBuilder) OrderBy(sql interface{}, args ...interface{}) *UpdateBuilder {
	if ub.orderBys == nil {
		ub.orderBys = NewClauses(',')
	}
	ub.orderBys.Append(sql, args...)
	return ub
}

func (ub *UpdateBuilder) Limit(limit int64) *UpdateBuilder {
	ub.limit = NewClause(" LIMIT ?", limit)
	return ub
}

func (ub *UpdateBuilder) Suffix(sql interface{}, args ...interface{}) *UpdateBuilder {
	if ub.suffixes == nil {
		ub.suffixes = NewClauses(' ')
	}
	ub.suffixes.Append(sql, args...)
	return ub
}

func (ub *UpdateBuilder) Write(w Writer) (err error) {
	if len(ub.table) == 0 {
		return errors.New("dbs: update clause must specify a table")
	}
	if len(ub.sets) == 0 {
		return errors.New("dbs: update clause must have at least one set")
	}
	if !ub.wheres.valid() {
		return errors.New("dbs: update clause must have at least one where")
	}

	if ub.prefixes.valid() {
		if err = ub.prefixes.Write(w); err != nil {
			return err
		}
		if err = w.WriteByte(' '); err != nil {
			return err
		}
	}

	if _, err = w.WriteString("UPDATE "); err != nil {
		return err
	}

	if ub.options.valid() {
		if err = ub.options.Write(w); err != nil {
			return err
		}
		if err = w.WriteByte(' '); err != nil {
			return err
		}
	}

	if _, err = w.WriteString(ub.table); err != nil {
		return err
	}

	if _, err = w.WriteString(" SET "); err != nil {
		return err
	}

	for idx, expr := range ub.sets {
		if idx != 0 {
			if err = w.WriteByte(','); err != nil {
				return err
			}
		}
		if err = expr.Write(w); err != nil {
			return err
		}
	}

	if ub.wheres.valid() {
		if _, err = w.WriteString(" WHERE "); err != nil {
			return err
		}
		if err = ub.wheres.Write(w); err != nil {
			return err
		}
	}

	if ub.orderBys.valid() {
		if _, err = w.WriteString(" ORDER BY "); err != nil {
			return err
		}

		if err = ub.orderBys.Write(w); err != nil {
			return err
		}
	}

	if ub.limit != nil {
		if err = ub.limit.Write(w); err != nil {
			return err
		}
	}

	if ub.suffixes.valid() {
		if err = w.WriteByte(' '); err != nil {
			return err
		}
		if err = ub.suffixes.Write(w); err != nil {
			return err
		}
	}

	return nil
}

func (ub *UpdateBuilder) SQL() (string, []interface{}, error) {
	var buffer = getBuffer()
	buffer.UsePlaceholder(ub.placeholder)

	defer putBuffer(buffer)

	if err := ub.Write(buffer); err != nil {
		return "", nil, err
	}
	return buffer.String(), buffer.Arguments(), nil
}

func (ub *UpdateBuilder) Scan(ctx context.Context, dest interface{}) error {
	return scan(ctx, ub.session, ub, dest)
}

func (ub *UpdateBuilder) Query(ctx context.Context) (*sql.Rows, error) {
	return query(ctx, ub.session, ub)
}

func (ub *UpdateBuilder) Exec(ctx context.Context) (sql.Result, error) {
	return exec(ctx, ub.session, ub)
}
