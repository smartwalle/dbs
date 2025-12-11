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
	wheres      *Conds
	groupBys    Parts
	having      *Clauses
	orderBys    *Clauses
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
		sb.prefixes = NewClauses(' ')
	}
	sb.prefixes.Append(sql, args...)
	return sb
}

func (sb *SelectBuilder) Option(sql interface{}, args ...interface{}) *SelectBuilder {
	if sb.options == nil {
		sb.options = NewClauses(' ')
	}
	sb.options.Append(sql, args...)
	return sb
}

func (sb *SelectBuilder) Selects(columns ...string) *SelectBuilder {
	return sb.Select(Parts(columns))
}

func (sb *SelectBuilder) Select(sql interface{}, args ...interface{}) *SelectBuilder {
	if sb.columns == nil {
		sb.columns = NewClauses(',')
	}
	sb.columns.Append(sql, args...)
	return sb
}

func (sb *SelectBuilder) From(table string, args ...interface{}) *SelectBuilder {
	if sb.tables == nil {
		sb.tables = NewClauses(',')
	}
	sb.tables.Append(table, args...)
	return sb
}

func (sb *SelectBuilder) Join(sql interface{}, args ...interface{}) *SelectBuilder {
	if sb.joins == nil {
		sb.joins = NewClauses(' ')
	}
	sb.joins.Append(sql, args...)
	return sb
}

func (sb *SelectBuilder) Where(sql interface{}, args ...interface{}) *SelectBuilder {
	if sb.wheres == nil {
		var conds = AND()
		conds.ignoreBracket = true
		sb.wheres = conds
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
		sb.having = NewClauses(' ')
	}
	sb.having.Append(sql, args...)
	return sb
}

func (sb *SelectBuilder) OrderBy(sql interface{}, args ...interface{}) *SelectBuilder {
	if sb.orderBys == nil {
		sb.orderBys = NewClauses(',')
	}
	sb.orderBys.Append(sql, args...)
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
		sb.suffixes = NewClauses(' ')
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
		if err = w.WriteByte(' '); err != nil {
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
		if err = w.WriteByte(' '); err != nil {
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
		if err = w.WriteByte(' '); err != nil {
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
		if err = sb.groupBys.Write(w); err != nil {
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

	if sb.orderBys.valid() {
		if _, err = w.WriteString(" ORDER BY "); err != nil {
			return err
		}
		if err = sb.orderBys.Write(w); err != nil {
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
		if err = w.WriteByte(' '); err != nil {
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

func (sb *SelectBuilder) Count() *SelectBuilder {
	if len(sb.groupBys) > 0 {
		return sb.EmbedCount()
	}

	var temp = *sb
	temp.limit = nil
	temp.offset = nil
	temp.orderBys = nil
	temp.columns = NewClauses(',', SQL("COUNT(1)"))
	return &temp
}

// EmbedCount 把待统计行数的查询语句嵌入到另一个查询语句中进行行数的统计
//
// 适用于有 GROUP BY 或 DISTINCT 的查询语句
//
// 如：
//
// 统计出每一个年龄对应的用户数量：
//
// SELECT u.age,COUNT(1) age_count FROM user u GROUP BY u.age
//
// 然后需要统计共有多少行数据：
//
// SELECT COUNT(1) FROM (SELECT u.age,COUNT(1) age_count FROM user u GROUP BY u.age) embed_count_table。
func (sb *SelectBuilder) EmbedCount(columns ...string) *SelectBuilder {
	var temp = *sb
	temp.limit = nil
	temp.offset = nil
	temp.orderBys = nil
	if len(columns) > 0 {
		temp.columns = NewClauses(',', Parts(columns))
	}

	var nsb = NewSelectBuilder()
	nsb.Selects("COUNT(1)")
	nsb.From("(?) embed_count_table", &temp)
	return nsb
}

func (sb *SelectBuilder) Scan(ctx context.Context, dest interface{}) error {
	return scan(ctx, sb.session, sb, dest)
}

func (sb *SelectBuilder) Query(ctx context.Context) (*sql.Rows, error) {
	return query(ctx, sb.session, sb)
}
