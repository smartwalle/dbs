package dbs

import (
	"context"
	"database/sql"
	"errors"
)

type SelectBuilder struct {
	placeholder Placeholder
	session     Session
	prefixes    *Clauses
	options     *Clauses
	columns     *Clauses
	tables      *Clauses
	joins       *Clauses
	wheres      *Clauses
	groupBys    Strings
	having      *Clauses
	orderBys    Strings
	limit       SQLClause
	offset      SQLClause
	suffixes    *Clauses
}

func NewSelectBuilder() *SelectBuilder {
	var sb = &SelectBuilder{}
	sb.placeholder = GlobalPlaceholder()
	return sb
}

func (sb *SelectBuilder) UsePlaceholder(p Placeholder) *SelectBuilder {
	sb.placeholder = p
	return sb
}

func (sb *SelectBuilder) UseSession(s Session) *SelectBuilder {
	sb.session = s
	return sb
}

func (sb *SelectBuilder) Prefix(sql interface{}, args ...interface{}) *SelectBuilder {
	if sb.prefixes == nil {
		sb.prefixes = NewClauses(" ")
	}
	sb.prefixes.Append(sql, args...)
	return sb
}

func (sb *SelectBuilder) Option(sql interface{}, args ...interface{}) *SelectBuilder {
	if sb.options == nil {
		sb.options = NewClauses(" ")
	}
	sb.options.Append(sql, args...)
	return sb
}

func (sb *SelectBuilder) Selects(columns ...string) *SelectBuilder {
	return sb.Select(NewColumns(", ", columns...))
}

func (sb *SelectBuilder) Select(sql interface{}, args ...interface{}) *SelectBuilder {
	if sb.columns == nil {
		sb.columns = NewClauses(", ")
	}
	sb.columns.Append(sql, args...)
	return sb
}

func (sb *SelectBuilder) From(table string, args ...interface{}) *SelectBuilder {
	if sb.tables == nil {
		sb.tables = NewClauses(", ")
	}
	sb.tables.Append(table, args...)
	return sb
}

func (sb *SelectBuilder) Join(sql interface{}, args ...interface{}) *SelectBuilder {
	if sb.joins == nil {
		sb.joins = NewClauses(" ")
	}
	sb.joins.Append(sql, args...)
	return sb
}

func (sb *SelectBuilder) Where(sql interface{}, args ...interface{}) *SelectBuilder {
	if sb.wheres == nil {
		sb.wheres = NewClauses(" AND ")
	}
	sb.wheres.Append(sql, args...)
	return sb
}

func (sb *SelectBuilder) GroupBy(groupBys ...string) *SelectBuilder {
	sb.groupBys = append(sb.groupBys, groupBys...)
	return sb
}

func (sb *SelectBuilder) Having(sql interface{}, args ...interface{}) *SelectBuilder {
	if sb.having == nil {
		sb.having = NewClauses(" ")
	}
	sb.having.Append(sql, args...)
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
	if sb.suffixes == nil {
		sb.suffixes = NewClauses(" ")
	}
	sb.suffixes.Append(sql, args...)
	return sb
}

func (sb *SelectBuilder) Write(w Writer) (err error) {
	if !sb.columns.valid() {
		return errors.New("dbs: select clause must have at least one result column")
	}

	if sb.prefixes.valid() {
		if err = sb.prefixes.Write(w); err != nil {
			return err
		}
		if _, err = w.WriteString(" "); err != nil {
			return err
		}
	}

	if _, err = w.WriteString("SELECT "); err != nil {
		return err
	}

	if sb.options.valid() {
		if err = sb.options.Write(w); err != nil {
			return err
		}
		if _, err = w.WriteString(" "); err != nil {
			return err
		}
	}

	if sb.columns.valid() {
		if err = sb.columns.Write(w); err != nil {
			return err
		}
	}

	if sb.tables.valid() {
		if _, err = w.WriteString(" FROM "); err != nil {
			return err
		}
		if err = sb.tables.Write(w); err != nil {
			return err
		}
	}

	if sb.joins.valid() {
		if _, err = w.WriteString(" "); err != nil {
			return err
		}
		if err = sb.joins.Write(w); err != nil {
			return err
		}
	}

	if sb.wheres.valid() {
		if _, err = w.WriteString(" WHERE "); err != nil {
			return err
		}
		if err = sb.wheres.Write(w); err != nil {
			return err
		}
	}

	if len(sb.groupBys) > 0 {
		if _, err = w.WriteString(" GROUP BY "); err != nil {
			return err
		}
		if err = sb.groupBys.Write(w, ", "); err != nil {
			return err
		}
	}

	if sb.having.valid() {
		if _, err = w.WriteString(" HAVING "); err != nil {
			return err
		}
		if err = sb.having.Write(w); err != nil {
			return err
		}
	}

	if len(sb.orderBys) > 0 {
		if _, err = w.WriteString(" ORDER BY "); err != nil {
			return err
		}
		if err = sb.orderBys.Write(w, ", "); err != nil {
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

	if sb.suffixes.valid() {
		if _, err = w.WriteString(" "); err != nil {
			return err
		}
		if err = sb.suffixes.Write(w); err != nil {
			return err
		}
	}

	return nil
}

func (sb *SelectBuilder) SQL() (string, []interface{}, error) {
	var buffer = getBuffer()
	buffer.UsePlaceholder(sb.placeholder)

	defer putBuffer(buffer)

	if err := sb.Write(buffer); err != nil {
		return "", nil, err
	}
	return buffer.String(), buffer.Arguments(), nil
}

func (sb *SelectBuilder) Scan(ctx context.Context, dst interface{}) error {
	return scan(ctx, sb.session, sb, dst)
}

func (sb *SelectBuilder) Query(ctx context.Context) (*sql.Rows, error) {
	return query(ctx, sb.session, sb)
}
