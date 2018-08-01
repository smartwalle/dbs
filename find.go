package dbs

import "database/sql"

// --------------------------------------------------------------------------------
func Find(s Executor, table string, dest interface{}, w Statement) (err error) {
	var sb = NewSelectBuilder()
	sb.Selects("*")
	sb.From(table)
	if w != nil {
		sb.Where(w)
	}
	return sb.Scan(s, dest)
}

func FindAll(s Executor, table string, dest interface{}) (err error) {
	return Find(s, table, dest, nil)
}

// --------------------------------------------------------------------------------
func Update(s Executor, table string, data map[string]interface{}, w Statement) (result sql.Result, err error) {
	var ub = NewUpdateBuilder()
	ub.Table(table)
	for k, v := range data {
		ub.SET(k, v)
	}
	if w != nil {
		ub.Where(w)
	}
	return ub.Exec(s)
}

// --------------------------------------------------------------------------------
func Insert(s Executor, table string, data map[string]interface{}) (result sql.Result, err error) {
	var ib = NewInsertBuilder()
	ib.Table(table)
	for k, v := range data {
		ib.SET(k, v)
	}
	return ib.Exec(s)
}

// --------------------------------------------------------------------------------
func Delete(s Executor, table string, w Statement) (result sql.Result, err error) {
	var db = NewDeleteBuilder()
	db.Table(table)
	if w != nil {
		db.Where(w)
	}
	return db.Exec(s)
}
