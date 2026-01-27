package dbs

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

type Builder struct {
	dialect Dialect
	session Session
	clauses *Clauses
}

func NewBuilder() *Builder {
	var sb = &Builder{}
	return sb
}

func (rb *Builder) Reset() {
	rb.dialect = nil
	rb.session = nil
	rb.clauses.reset()
}

func (rb *Builder) UseDialect(dialect Dialect) *Builder {
	rb.dialect = dialect
	return rb
}

func (rb *Builder) UseSession(session Session) *Builder {
	rb.session = session
	if rb.session != nil {
		rb.dialect = rb.session.Dialect()
	}
	return rb
}

func (rb *Builder) Append(sql any, args ...any) *Builder {
	if rb.clauses == nil {
		rb.clauses = NewClauses(' ')
	}
	rb.clauses.Append(sql, args...)
	return rb
}

func (rb *Builder) Raw(sql string, args ...any) *Builder {
	if rb.clauses == nil {
		rb.clauses = NewClauses(' ')
	}
	rb.clauses.Append(sql, args...)
	return rb
}

func (rb *Builder) Write(w Writer) (err error) {
	if rb.clauses.valid() {
		if err = rb.clauses.Write(w); err != nil {
			return err
		}
	}
	return nil
}

func (rb *Builder) SQL() (string, []any, error) {
	var buffer = NewBuffer()
	defer buffer.Release()

	buffer.UseDialect(rb.dialect)

	if err := rb.Write(buffer); err != nil {
		return "", nil, err
	}
	return buffer.String(), buffer.Arguments(), nil
}

func (rb *Builder) Scan(ctx context.Context, dest any) error {
	return scan(ctx, rb.session, rb, dest)
}

func (rb *Builder) Exec(ctx context.Context) (sql.Result, error) {
	return exec(ctx, rb.session, rb)
}

func scan(ctx context.Context, session Session, clause SQLClause, dest any) (err error) {
	var query string
	var args []any
	var rowsAffected int
	var logger = session.Logger()
	if logger != nil {
		var beginTime = time.Now()
		defer func() {
			logger.Trace(ctx, 4, beginTime, query, args, int64(rowsAffected), err)
		}()
	}

	if query, args, err = clause.SQL(); err != nil {
		return err
	}

	var rows *sql.Rows
	rows, err = session.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	if rowsAffected, err = session.Mapper().Decode(rows, dest); err != nil && !errors.Is(err, ErrNoRows) {
		return err
	}
	return nil
}

func exec(ctx context.Context, session Session, clause SQLClause) (result sql.Result, err error) {
	var query string
	var args []any
	var logger = session.Logger()
	if logger != nil {
		var beginTime = time.Now()
		defer func() {
			var rowsAffected int64
			if result != nil {
				rowsAffected, _ = result.RowsAffected()
			}
			logger.Trace(ctx, 4, beginTime, query, args, rowsAffected, err)
		}()
	}

	if query, args, err = clause.SQL(); err != nil {
		return nil, err
	}
	return session.ExecContext(ctx, query, args...)
}
