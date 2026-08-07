package postgres_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/smartwalle/dbs"
)

func TestSessionFromContextNil(t *testing.T) {
	// SessionFromContext 对空 context 应返回 nil
	session := dbs.SessionFromContext(context.Background())
	if session != nil {
		t.Fatalf("SessionFromContext on empty context should return nil, got %T", session)
	}
}

func TestContextWithSessionRoundTrip(t *testing.T) {
	db := open(t)

	ctx := dbs.ContextWithSession(context.Background(), db)
	session := dbs.SessionFromContext(ctx)
	if session != db {
		t.Fatalf("round-trip failed: want %p, got %p", db, session)
	}
}

func TestDBSessionNoTransaction(t *testing.T) {
	db := open(t)

	// 没有事务时，DB.Session(ctx) 返回 *DB 自身
	session := db.Session(context.Background())
	if _, ok := session.(*dbs.DB); !ok {
		t.Fatalf("DB.Session without tx should return *dbs.DB, got %T", session)
	}
}

func TestDBSessionInTransaction(t *testing.T) {
	db := open(t)
	repo := dbs.NewRepository[Mail](db)

	var sessionInTx dbs.Session
	err := repo.Transaction(context.Background(), func(ctx context.Context) error {
		// 事务内 DB.Session(ctx) 应返回 *Tx
		sessionInTx = db.Session(ctx)
		return nil
	}, nil)
	if err != nil {
		t.Fatal(err)
	}

	if _, ok := sessionInTx.(*dbs.Tx); !ok {
		t.Fatalf("DB.Session inside tx should return *dbs.Tx, got %T", sessionInTx)
	}
}

func TestRepositorySessionInTransaction(t *testing.T) {
	db := open(t)
	repo := dbs.NewRepository[Mail](db)

	var sessionInTx dbs.Session
	err := repo.Transaction(context.Background(), func(ctx context.Context) error {
		sessionInTx = repo.Database().Session(ctx)
		return nil
	}, nil)
	if err != nil {
		t.Fatal(err)
	}

	if _, ok := sessionInTx.(*dbs.Tx); !ok {
		t.Fatalf("Database().Session inside tx should return *dbs.Tx, got %T", sessionInTx)
	}
}

func TestSessionInNestedTransaction(t *testing.T) {
	db := open(t)
	repo := dbs.NewRepository[Mail](db)

	var outerSession, innerSession dbs.Session
	err := repo.Transaction(context.Background(), func(ctx context.Context) error {
		outerSession = db.Session(ctx)

		// 嵌套事务应复用外层 *Tx
		return repo.Transaction(ctx, func(innerCtx context.Context) error {
			innerSession = db.Session(innerCtx)
			return nil
		}, nil)
	}, nil)
	if err != nil {
		t.Fatal(err)
	}

	// 嵌套事务中获取的 session 应和外层的 *Tx 是同一个实例
	if outerSession != innerSession {
		t.Fatalf("nested tx session should be the same instance as outer tx session")
	}
}

func TestNestedTransactionSuccess(t *testing.T) {
	db := open(t)
	repo := dbs.NewRepository[Mail](db)
	now := time.Now().UTC().Truncate(time.Microsecond)

	ctx := context.Background()

	err := repo.Transaction(ctx, func(ctx context.Context) error {
		// 外层事务插入第 1 条
		outerMail := &Mail{
			Email:     "outer@example.com",
			Status:    "active",
			CreatedAt: &now,
			UpdatedAt: now,
		}
		if _, err := repo.Create(ctx, outerMail); err != nil {
			return fmt.Errorf("outer create: %w", err)
		}

		// 嵌套事务插入第 2 条
		return repo.Transaction(ctx, func(innerCtx context.Context) error {
			innerMail := &Mail{
				Email:     "inner@example.com",
				Status:    "active",
				CreatedAt: &now,
				UpdatedAt: now,
			}
			if _, err := repo.Create(innerCtx, innerMail); err != nil {
				return fmt.Errorf("inner create: %w", err)
			}
			return nil
		}, nil)
	}, nil)
	if err != nil {
		t.Fatalf("nested transaction should succeed: %v", err)
	}

	// 验证两条记录都落库
	mails, err := repo.FindList(ctx, "*", "email IN (?, ?)", "outer@example.com", "inner@example.com")
	if err != nil {
		t.Fatal(err)
	}
	if len(mails) != 2 {
		t.Fatalf("expected 2 mails after nested tx commit, got %d", len(mails))
	}
}

func TestNestedTransactionFailure(t *testing.T) {
	db := open(t)
	repo := dbs.NewRepository[Mail](db)
	now := time.Now().UTC().Truncate(time.Microsecond)

	ctx := context.Background()
	sentinelErr := errors.New("inner rollback on purpose")

	err := repo.Transaction(ctx, func(ctx context.Context) error {
		// 外层事务插入 1 条
		outerMail := &Mail{
			Email:     "outer-fail@example.com",
			Status:    "active",
			CreatedAt: &now,
			UpdatedAt: now,
		}
		if _, err := repo.Create(ctx, outerMail); err != nil {
			return fmt.Errorf("outer create: %w", err)
		}

		// 嵌套事务返回 error，连累外层回滚
		return repo.Transaction(ctx, func(innerCtx context.Context) error {
			return sentinelErr
		}, nil)
	}, nil)

	if !errors.Is(err, sentinelErr) {
		t.Fatalf("expected sentinel error from nested tx, got: %v", err)
	}

	// 外层插入的数据也应回滚，查不到
	mail, findErr := repo.FindOne(ctx, "*", "email = ?", "outer-fail@example.com")
	if findErr != nil {
		t.Fatal(findErr)
	}
	if mail != nil {
		t.Fatalf("expected nil after rollback, got email=%s", mail.Email)
	}
}
