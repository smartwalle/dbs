package dbs

import (
	"context"
	"database/sql"
	"errors"
)

type Builder struct {
	placeholder Placeholder
	session     Session
	clauses     *Clauses
}

func NewBuilder() *Builder {
	var sb = &Builder{}
	sb.placeholder = GlobalPlaceholder()
	return sb
}

func (rb *Builder) UsePlaceholder(p Placeholder) *Builder {
	rb.placeholder = p
	return rb
}

func (rb *Builder) UseSession(s Session) *Builder {
	rb.session = s
	return rb
}

func (rb *Builder) Append(sql interface{}, args ...interface{}) *Builder {
	if rb.clauses == nil {
		rb.clauses = NewClauses(' ')
	}
	rb.clauses.Append(sql, args...)
	return rb
}

func (rb *Builder) Raw(sql string, args ...interface{}) *Builder {
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

func (rb *Builder) SQL() (string, []interface{}, error) {
	var buffer = getBuffer()
	buffer.UsePlaceholder(rb.placeholder)

	defer putBuffer(buffer)

	if err := rb.Write(buffer); err != nil {
		return "", nil, err
	}
	return buffer.String(), buffer.Arguments(), nil
}

func (rb *Builder) Scan(ctx context.Context, dest interface{}) error {
	return scan(ctx, rb.session, rb, dest)
}

func (rb *Builder) Query(ctx context.Context) (*sql.Rows, error) {
	return query(ctx, rb.session, rb)
}

func (rb *Builder) Exec(ctx context.Context) (sql.Result, error) {
	return exec(ctx, rb.session, rb)
}

func scan(ctx context.Context, session Session, clause SQLClause, dest interface{}) error {
	rows, err := query(ctx, session, clause)
	if err != nil {
		return err
	}
	defer rows.Close()

	if err = gScanner.Scan(rows, dest); err != nil && !errors.Is(err, ErrNoRows) {
		return err
	}
	return nil
}

func query(ctx context.Context, session Session, clause SQLClause) (*sql.Rows, error) {
	sql, args, err := clause.SQL()
	if err != nil {
		return nil, err
	}

	rows, err := session.QueryContext(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	return rows, nil
}

func exec(ctx context.Context, session Session, clause SQLClause) (result sql.Result, err error) {
	sql, args, err := clause.SQL()
	if err != nil {
		return nil, err
	}
	return session.ExecContext(ctx, sql, args...)
}
