package dbs

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"time"
)

const (
	kTableMigration = "dbs_migrations"
)

const (
	kMigrateFileExt = ".sql"
)

const (
	kCreateMigrationTableSQL = "CREATE TABLE IF NOT EXISTS `%s` (`id` int(11) NOT NULL AUTO_INCREMENT, `name` varchar(256) DEFAULT NULL, `created_on` datetime DEFAULT NULL,PRIMARY KEY (`id`),UNIQUE KEY `dbs_migration_id_uindex` (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8"
)

type Migration struct {
	db    DB
	stmts map[string]string
}

func NewMigration(db DB) *Migration {
	var m = &Migration{}
	m.db = db
	m.stmts = make(map[string]string)
	return m
}

func (this *Migration) Load(filename string) error {
	var pathList []string

	filepath.Walk(filename, func(path string, info os.FileInfo, err error) error {
		if info.IsDir() == false && filepath.Ext(info.Name()) == kMigrateFileExt {
			pathList = append(pathList, path)
		}
		return nil
	})

	for _, fp := range pathList {
		var content, err = ioutil.ReadFile(fp)
		if err != nil {
			return err
		}
		if len(content) == 0 {
			continue
		}
		this.stmts[fp] = string(content)
	}
	return nil
}

func (this *Migration) Flush() (err error) {
	if len(this.stmts) == 0 {
		return nil
	}

	if err = this.initTable(); err != nil {
		return err
	}

	mList, err := this.getMigrateRecord()
	if err != nil {
		return err
	}

	tx, err := NewTx(this.db)
	if err != nil {
		return err
	}

	// 添加 migration 记录
	var ib = NewInsertBuilder()
	ib.Table(kTableMigration)
	ib.Columns("name", "created_on")

	var ibCount = 0

	var rb = NewBuilder("")
	for key, stmts := range this.stmts {
		var m = mList[key]
		if m != nil {
			continue
		}

		rb.reset()
		rb.Append(stmts)

		if _, err = rb.Exec(tx); err != nil {
			return err
		}

		ib.Values(key, time.Now())
		ibCount++

		if ibCount%5 == 0 {
			if _, err = ib.Exec(tx); err != nil {
				return err
			}
			ib.reset()
			ibCount = 0
		}
	}

	if ibCount > 0 {
		if _, err = ib.Exec(tx); err != nil {
			return err
		}
	}

	tx.Commit()

	return nil
}

func (this *Migration) initTable() error {
	var rb = NewBuilder("")
	rb.Format(kCreateMigrationTableSQL, kTableMigration)
	if _, err := rb.Exec(this.db); err != nil {
		return err
	}
	return nil
}

func (this *Migration) getMigrateRecord() (result map[string]*MigrateRecord, err error) {
	var mList []*MigrateRecord
	var sb = NewSelectBuilder()
	sb.From(kTableMigration, "AS m")
	sb.Selects("m.id", "m.name")
	if err = sb.Scan(this.db, &mList); err != nil {
		return nil, err
	}

	result = make(map[string]*MigrateRecord)
	for _, m := range mList {
		result[m.Name] = m
	}

	return result, nil
}

type MigrateRecord struct {
	Id   int64  `sql:"id"`
	Name string `sql:"name"`
}
