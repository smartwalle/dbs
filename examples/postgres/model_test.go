package postgres_test

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"time"
)

const mailTable = "dbs_example_mail"

type Base struct {
	Id int32 `sql:"id;default"`
}

type Mail struct {
	Base
	Email     string     `sql:"email"`
	Status    string     `sql:"status"`
	CreatedAt *time.Time `sql:"created_at"`
	UpdatedAt time.Time  `sql:"updated_at"`
	Extra     *Extra     `sql:"extra"`
}

func (Mail) TableName() string {
	return mailTable
}

func (Mail) PrimaryKey() string {
	return "id"
}

type Extra struct {
	Age  int    `json:"age"`
	City string `json:"city"`
	Name string `json:"name"`
}

func (a Extra) Value() (driver.Value, error) {
	return json.Marshal(a)
}

func (a *Extra) Scan(value any) error {
	if value == nil {
		return nil
	}

	var data []byte
	switch v := value.(type) {
	case []byte:
		data = v
	case string:
		data = []byte(v)
	default:
		return errors.New("type assertion to []byte failed")
	}
	return json.Unmarshal(data, a)
}
