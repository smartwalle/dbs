package dbs

import (
	"context"
	"database/sql"
	"sync"
)

type Stmt struct {
	stmt     *sql.Stmt
	prepared chan struct{}
	err      error
}

type Stmts struct {
	session Session
	mu      *sync.RWMutex
	stmts   map[string]*Stmt
}

func NewStmts(session Session) *Stmts {
	return &Stmts{
		session: session,
		mu:      &sync.RWMutex{},
		stmts:   make(map[string]*Stmt),
	}
}

func (s *Stmts) prepareStatement(ctx context.Context, preparer Preparer, inTransaction bool, key, query string) (*sql.Stmt, error) {
	s.mu.RLock()
	if stmt, exists := s.stmts[key]; exists {
		s.mu.RUnlock()
		<-stmt.prepared
		if stmt.err != nil {
			return nil, stmt.err
		}
		return stmt.stmt, nil
	}
	s.mu.RUnlock()
	if inTransaction {
		return preparer.PrepareContext(ctx, query)
	}

	s.mu.Lock()
	if stmt, exists := s.stmts[key]; exists {
		s.mu.Unlock()
		<-stmt.prepared
		if stmt.err != nil {
			return nil, stmt.err
		}
		return stmt.stmt, nil
	}

	var stmt = &Stmt{prepared: make(chan struct{})}
	s.stmts[key] = stmt
	s.mu.Unlock()

	defer close(stmt.prepared)

	nStmt, err := preparer.PrepareContext(ctx, query)
	if err != nil {
		stmt.err = err
		s.mu.Lock()
		delete(s.stmts, key)
		s.mu.Unlock()
		return nil, err
	}
	s.mu.Lock()
	stmt.stmt = nStmt
	s.mu.Unlock()
	return nStmt, nil
}

func (s *Stmts) RevokeStatement(key string) {
	s.mu.RLock()
	var stmt, exists = s.stmts[key]
	s.mu.RUnlock()

	if exists {
		<-stmt.prepared
		s.removeStatement(key, stmt.stmt)
	}
}

func (s *Stmts) removeStatement(key string, stmt *sql.Stmt) {
	s.mu.Lock()
	if stmt != nil {
		go stmt.Close()
	}
	delete(s.stmts, key)
	s.mu.Unlock()
}

func (s *Stmts) statement(ctx context.Context, query string) (*sql.Stmt, error) {
	return s.prepareStatement(ctx, s.session, false, query, query)
}

func (s *Stmts) Close() error {
	s.mu.Lock()
	for _, stmt := range s.stmts {
		go stmt.stmt.Close()
	}
	s.stmts = make(map[string]*Stmt)
	s.mu.Unlock()
	return nil
}

func (s *Stmts) Dialect() Dialect {
	return s.session.Dialect()
}

func (s *Stmts) Logger() Logger {
	return s.session.Logger()
}

func (s *Stmts) Mapper() Mapper {
	return s.session.Mapper()
}

func (s *Stmts) Prepare(query string) (*sql.Stmt, error) {
	return s.PrepareContext(context.Background(), query)
}

func (s *Stmts) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	return s.session.PrepareContext(ctx, query)
}

func (s *Stmts) Exec(query string, args ...interface{}) (sql.Result, error) {
	return s.ExecContext(context.Background(), query, args...)
}

func (s *Stmts) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	stmt, err := s.statement(ctx, query)
	if err != nil {
		return nil, err
	}
	result, err := stmt.ExecContext(ctx, args...)
	if err != nil {
		s.removeStatement(query, stmt)
	}
	return result, err
}

func (s *Stmts) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return s.QueryContext(context.Background(), query, args...)
}

func (s *Stmts) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	stmt, err := s.statement(ctx, query)
	if err != nil {
		return nil, err
	}
	rows, err := stmt.QueryContext(ctx, args...)
	if err != nil {
		s.removeStatement(query, stmt)
	}
	return rows, err
}

func (s *Stmts) QueryRow(query string, args ...any) *sql.Row {
	return s.QueryRowContext(context.Background(), query, args...)
}

func (s *Stmts) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	stmt, err := s.statement(ctx, query)
	if err != nil {
		return nil
	}
	row := stmt.QueryRowContext(ctx, args...)
	if row.Err() != nil {
		s.removeStatement(query, stmt)
	}
	return row
}
