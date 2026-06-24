package dbs

import (
	"context"
	"database/sql"
)

const (
	kRepositoryDepth  = 5
	kDefaultBatchSize = 100
)

type Entity interface {
	TableName() string

	PrimaryKey() string
}

type Repository[E Entity] interface {
	TableName() string

	Database() Database

	InsertBuilder(ctx context.Context) *InsertBuilder

	DeleteBuilder(ctx context.Context) *DeleteBuilder

	UpdateBuilder(ctx context.Context) *UpdateBuilder

	SelectBuilder(ctx context.Context) *SelectBuilder

	Create(ctx context.Context, entity *E) (sql.Result, error)

	CreateInBatches(ctx context.Context, batchSize int, entities ...*E) (sql.Result, error)

	Delete(ctx context.Context, id any) (sql.Result, error)

	Update(ctx context.Context, id any, values map[string]any) (sql.Result, error)

	Find(ctx context.Context, id any, columns string) (*E, error)

	FindOne(ctx context.Context, columns, conds string, args ...any) (*E, error)

	FindList(ctx context.Context, columns, conds string, args ...any) ([]*E, error)

	FindOrderedList(ctx context.Context, columns, orderBy, conds string, args ...any) ([]*E, error)

	Transaction(ctx context.Context, fn func(ctx context.Context) error, opts ...*sql.TxOptions) error
}

type repository[E Entity] struct {
	entity E
	db     Database
}

func NewRepository[E Entity](db Database) Repository[E] {
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
	var columns = make([]string, 0, len(fieldValues))
	var values = make([]any, 0, len(fieldValues))

	for _, fieldValue := range fieldValues {
		if fieldValue.UseDefault {
			continue
		}
		columns = append(columns, fieldValue.Name)
		values = append(values, fieldValue.Value)
	}

	var ib = r.InsertBuilder(ctx)
	ib.Columns(columns...)
	ib.Values(values...)
	return ib.Exec(withDepth(ctx, kRepositoryDepth))
}

func (r *repository[E]) CreateInBatches(ctx context.Context, batchSize int, entities ...*E) (sql.Result, error) {
	if len(entities) == 0 {
		return insertResults(nil), nil
	}
	if batchSize <= 0 {
		batchSize = kDefaultBatchSize
	}

	var columns []string
	var rows = make([][]any, 0, len(entities))

	for idx, entity := range entities {
		var fieldValues, err = r.db.Mapper().Encode(entity)
		if err != nil {
			return nil, err
		}

		if idx == 0 {
			columns = make([]string, len(fieldValues))
		}

		var values = make([]any, len(fieldValues))
		for i, fieldValue := range fieldValues {
			if idx == 0 {
				columns[i] = fieldValue.Name
			}
			values[i] = fieldValue.Value
		}
		rows = append(rows, values)
	}

	var results insertResults
	var err = r.Transaction(ctx, func(ctx context.Context) error {
		for start := 0; start < len(rows); start += batchSize {
			var end = start + batchSize
			if end > len(rows) {
				end = len(rows)
			}

			var ib = r.InsertBuilder(ctx)
			ib.Columns(columns...)
			for _, values := range rows[start:end] {
				ib.Values(values...)
			}

			var result, err = ib.Exec(withDepth(ctx, kRepositoryDepth+2))
			if err != nil {
				return err
			}
			results = append(results, result)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return results, nil
}

func (r *repository[E]) Delete(ctx context.Context, id any) (sql.Result, error) {
	var rb = r.DeleteBuilder(ctx)
	rb.Where(r.entity.PrimaryKey()+" = ?", id)
	return rb.Exec(withDepth(ctx, kRepositoryDepth))
}

func (r *repository[E]) Update(ctx context.Context, id any, values map[string]any) (sql.Result, error) {
	var ub = r.UpdateBuilder(ctx)
	ub.SetValues(values)
	ub.Where(r.entity.PrimaryKey()+" = ?", id)
	return ub.Exec(withDepth(ctx, kRepositoryDepth))
}

func (r *repository[E]) Find(ctx context.Context, id any, columns string) (entity *E, err error) {
	var sb = r.SelectBuilder(ctx)
	sb.Selects(columns)
	sb.Limit(1)
	sb.Where(r.entity.PrimaryKey()+" = ?", id)

	if err = sb.Scan(withDepth(ctx, kRepositoryDepth), &entity); err != nil {
		return nil, err
	}
	return entity, nil
}

func (r *repository[E]) FindOne(ctx context.Context, columns string, conds string, args ...any) (entity *E, err error) {
	var sb = r.SelectBuilder(ctx)
	sb.Selects(columns)
	sb.Limit(1)
	sb.Where(conds, args...)

	if err = sb.Scan(withDepth(ctx, kRepositoryDepth), &entity); err != nil {
		return nil, err
	}
	return entity, nil
}

func (r *repository[E]) FindList(ctx context.Context, columns, conds string, args ...any) (entityList []*E, err error) {
	var sb = r.SelectBuilder(ctx)
	sb.Selects(columns)
	sb.Where(conds, args...)

	if err = sb.Scan(withDepth(ctx, kRepositoryDepth), &entityList); err != nil {
		return nil, err
	}
	return entityList, nil
}

func (r *repository[E]) FindOrderedList(ctx context.Context, columns, orderBy, conds string, args ...any) (entityList []*E, err error) {
	var sb = r.SelectBuilder(ctx)
	sb.Selects(columns)
	sb.OrderBy(orderBy)
	sb.Where(conds, args...)

	if err = sb.Scan(withDepth(ctx, kRepositoryDepth), &entityList); err != nil {
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
				_ = tx.Rollback()
			}
		}()

		if err = fn(tx.WithContext(ctx)); err != nil {
			return err
		}
		return tx.Commit()
	}

	return fn(ctx)
}

type insertResults []sql.Result

func (rs insertResults) LastInsertId() (int64, error) {
	if len(rs) == 0 {
		return 0, nil
	}
	return rs[len(rs)-1].LastInsertId()
}

func (rs insertResults) RowsAffected() (int64, error) {
	var rowsAffected int64
	for _, result := range rs {
		if result == nil {
			continue
		}
		var n, err = result.RowsAffected()
		if err != nil {
			return rowsAffected, err
		}
		rowsAffected += n
	}
	return rowsAffected, nil
}
