package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/smartwalle/dbs"
)

func TestRepositoryFind(t *testing.T) {
	db := open(t)
	repo := dbs.NewRepository[Mail](db)

	mail, err := repo.Find(context.Background(), 1, "*")
	if err != nil {
		t.Fatal(err)
	}
	if mail == nil || mail.Id != 1 {
		t.Fatalf("mail id = %v, want 1", mail)
	}
}

func TestRepositoryFindList(t *testing.T) {
	db := open(t)
	repo := dbs.NewRepository[Mail](db)

	mails, err := repo.FindList(context.Background(), "*", "id > ?", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(mails) != 4 {
		t.Fatalf("len(mails) = %d, want 4", len(mails))
	}
}

func TestRepositoryCreate(t *testing.T) {
	db := open(t)
	repo := dbs.NewRepository[Mail](db)

	now := time.Now().UTC().Truncate(time.Microsecond)
	mail := &Mail{
		Email:     "created@example.com",
		Status:    "active",
		CreatedAt: &now,
		UpdatedAt: now,
		Extra: &Extra{
			Age:  30,
			City: "Hangzhou",
			Name: "created",
		},
	}
	if _, err := repo.Create(context.Background(), mail); err != nil {
		t.Fatal(err)
	}

	mails, err := repo.FindList(context.Background(), "*", "email = ?", mail.Email)
	if err != nil {
		t.Fatal(err)
	}
	if len(mails) != 1 {
		t.Fatalf("len(mails) = %d, want 1", len(mails))
	}
}

func TestRepositoryTransaction(t *testing.T) {
	db := open(t)
	repo := dbs.NewRepository[Mail](db)

	err := repo.Transaction(context.Background(), func(ctx context.Context) error {
		now := time.Now().UTC().Truncate(time.Microsecond)
		mail := &Mail{
			Email:     "transaction@example.com",
			Status:    "active",
			CreatedAt: &now,
			UpdatedAt: now,
			Extra: &Extra{
				Age:  32,
				City: "Shanghai",
				Name: "transaction",
			},
		}
		_, err := repo.Create(ctx, mail)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	mails, err := repo.FindList(context.Background(), "*", "email = ?", "transaction@example.com")
	if err != nil {
		t.Fatal(err)
	}
	if len(mails) != 1 {
		t.Fatalf("len(mails) = %d, want 1", len(mails))
	}
}
