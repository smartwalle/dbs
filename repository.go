package dbs

import (
	"context"
	"database/sql"
)

type Entity interface {
	TableName() string

	PrimaryKey() string
}

type Repository[E Entity] struct {
	entity  E
	dialect Dialect
	db      Database
}

func NewRepository[E Entity](db Database) *Repository[E] {
	var r = &Repository[E]{}
	r.db = db
	return r
}

func (r *Repository[E]) UseDialect(dialect Dialect) {
	r.dialect = dialect
}

func (r *Repository[E]) Insert(ctx context.Context, entity *E) (sql.Result, error) {
	var fieldValues, err = GlobalMapper().Encode(entity)
	if err != nil {
		return nil, err
	}
	var columns = make([]string, len(fieldValues))
	var values = make([]interface{}, len(fieldValues))

	for idx, fieldValue := range fieldValues {
		columns[idx] = fieldValue.Name
		values[idx] = fieldValue.Value
	}

	var ib = NewInsertBuilder()
	ib.UseDialect(r.dialect)
	ib.UseSession(r.db.Session(ctx))

	ib.Table(r.entity.TableName())
	ib.Columns(columns...)
	ib.Values(values...)

	return ib.Exec(ctx)
}

func (r *Repository[E]) Delete(ctx context.Context, id interface{}) (sql.Result, error) {
	var rb = NewDeleteBuilder()
	rb.UseDialect(r.dialect)
	rb.UseSession(r.db.Session(ctx))

	rb.Table(r.entity.TableName())
	rb.Where(r.entity.PrimaryKey()+" = ?", id)

	return rb.Exec(ctx)
}

func (r *Repository[E]) Update(ctx context.Context, id interface{}, values map[string]interface{}) (sql.Result, error) {
	var ub = NewUpdateBuilder()
	ub.UseDialect(r.dialect)
	ub.UseSession(r.db.Session(ctx))

	ub.Table(r.entity.TableName())
	ub.SetValues(values)
	ub.Where(r.entity.PrimaryKey()+" = ?", id)

	return ub.Exec(ctx)
}

func (r *Repository[E]) Select(ctx context.Context, id interface{}, columns ...string) (*E, error) {
	var sb = NewSelectBuilder()
	sb.UseDialect(r.dialect)
	sb.UseSession(r.db.Session(ctx))

	sb.Table(r.entity.TableName())
	if len(columns) > 0 {
		sb.Selects(columns...)
	} else {
		sb.Selects("*")
	}
	sb.Where(r.entity.PrimaryKey()+" = ?", id)

	var entity *E
	if err := sb.Scan(ctx, &entity); err != nil {
		return nil, err
	}
	return entity, nil
}
