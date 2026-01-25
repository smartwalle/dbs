package dbs

import (
	"context"
	"database/sql"
)

type Entity interface {
	TableName() string

	PrimaryKey() string
}

type Repositoy[E Entity] interface {
	TableName() string

	Database() Database

	InsertBuilder(ctx context.Context) *InsertBuilder

	DeleteBuilder(ctx context.Context) *DeleteBuilder

	UpdateBuilder(ctx context.Context) *UpdateBuilder

	SelectBuilder(ctx context.Context) *SelectBuilder

	Create(ctx context.Context, entity *E) (sql.Result, error)

	Delete(ctx context.Context, id any) (sql.Result, error)

	Update(ctx context.Context, id any, values map[string]any) (sql.Result, error)

	Find(ctx context.Context, id any, columns string) (*E, error)

	FindOne(ctx context.Context, columns string, conds string, args ...any) (*E, error)

	FindList(ctx context.Context, columns string, conds string, args ...any) ([]*E, error)

	Transaction(ctx context.Context, fn func(ctx context.Context) error, opts ...*sql.TxOptions) error
}

type repository[E Entity] struct {
	entity E
	db     Database
}

func NewRepository[E Entity](db Database) Repositoy[E] {
	var r = &repository[E]{}
	r.db = db
	return r
}

func (r *repository[E]) TableName() string {
	return r.entity.TableName()
}

func (r *repository[E]) Database() Database {
	return r.db
}

func (r *repository[E]) InsertBuilder(ctx context.Context) *InsertBuilder {
	var ib = NewInsertBuilder()
	ib.UseSession(r.Database().Session(ctx))
	ib.Table(r.TableName())
	return ib
}

func (r *repository[E]) DeleteBuilder(ctx context.Context) *DeleteBuilder {
	var rb = NewDeleteBuilder()
	rb.UseSession(r.Database().Session(ctx))
	rb.Table(r.TableName())
	return rb
}

func (r *repository[E]) UpdateBuilder(ctx context.Context) *UpdateBuilder {
	var ub = NewUpdateBuilder()
	ub.UseSession(r.Database().Session(ctx))
	ub.Table(r.TableName())
	return ub
}

func (r *repository[E]) SelectBuilder(ctx context.Context) *SelectBuilder {
	var sb = NewSelectBuilder()
	sb.UseSession(r.Database().Session(ctx))
	sb.Table(r.TableName())
	return sb
}

func (r *repository[E]) Create(ctx context.Context, entity *E) (sql.Result, error) {
	var fieldValues, err = r.db.Mapper().Encode(entity)
	if err != nil {
		return nil, err
	}
	var columns = make([]string, len(fieldValues))
	var values = make([]any, len(fieldValues))

	for idx, fieldValue := range fieldValues {
		columns[idx] = fieldValue.Name
		values[idx] = fieldValue.Value
	}

	var ib = r.InsertBuilder(ctx)
	ib.Columns(columns...)
	ib.Values(values...)
	return ib.Exec(ctx)
}

func (r *repository[E]) Delete(ctx context.Context, id any) (sql.Result, error) {
	var rb = r.DeleteBuilder(ctx)
	rb.Where(r.entity.PrimaryKey()+" = ?", id)
	return rb.Exec(ctx)
}

func (r *repository[E]) Update(ctx context.Context, id any, values map[string]any) (sql.Result, error) {
	var ub = r.UpdateBuilder(ctx)
	ub.SetValues(values)
	ub.Where(r.entity.PrimaryKey()+" = ?", id)
	return ub.Exec(ctx)
}

func (r *repository[E]) Find(ctx context.Context, id any, columns string) (entity *E, err error) {
	var sb = r.SelectBuilder(ctx)
	sb.Selects(columns)
	sb.Limit(1)
	sb.Where(r.entity.PrimaryKey()+" = ?", id)

	if err = sb.Scan(ctx, &entity); err != nil {
		return nil, err
	}
	return entity, nil
}

func (r *repository[E]) FindOne(ctx context.Context, columns string, conds string, args ...any) (entity *E, err error) {
	var sb = r.SelectBuilder(ctx)
	sb.Selects(columns)
	sb.Limit(1)
	sb.Where(conds, args...)

	if err = sb.Scan(ctx, &entity); err != nil {
		return nil, err
	}
	return entity, nil
}

func (r *repository[E]) FindList(ctx context.Context, columns string, conds string, args ...any) (entityList []*E, err error) {
	var sb = r.SelectBuilder(ctx)
	sb.Selects(columns)
	sb.Where(conds, args...)

	if err = sb.Scan(ctx, &entityList); err != nil {
		return nil, err
	}
	return entityList, nil
}

func (r *repository[E]) Transaction(ctx context.Context, fn func(ctx context.Context) error, opts ...*sql.TxOptions) (err error) {
	var tx = TxFromContext(ctx)
	if tx == nil {
		var opt *sql.TxOptions
		if len(opts) > 0 {
			opt = opts[0]
		}
		tx, err = r.db.BeginTx(ctx, opt)
		if err != nil {
			return err
		}
		defer func() {
			if err != nil {
				tx.Rollback()
			}
		}()

		if err = fn(tx.ToContext(ctx)); err != nil {
			return err
		}
		return tx.Commit()
	}

	return fn(ctx)
}
