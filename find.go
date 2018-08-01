package dbs

import (
	"database/sql"
	"errors"
	"reflect"
	"sync"
)

var tagMap = sync.Map{}

func getTagList(dest interface{}) (result []string, err error) {
	var nDest = dest
	var destType = reflect.TypeOf(nDest)
	var destValue = reflect.ValueOf(nDest)
	var destValueKind = destValue.Kind()

	var key = destType.String()
	if value, ok := tagMap.Load(key); ok {
		if tags, ok := value.([]string); ok {
			return tags, nil
		}
	}

	if destValueKind == reflect.Struct {
		return nil, errors.New("dest argument is struct")
	}

	if destValue.IsNil() {
		return nil, errors.New("dest argument is nil")
	}

	for {
		if destValueKind == reflect.Ptr && destValue.IsNil() {
			destValue.Set(reflect.New(destType.Elem()))
		}

		if destValueKind == reflect.Ptr {
			destValue = destValue.Elem()
			destType = destType.Elem()
			destValueKind = destValue.Kind()
			continue
		} else if destValueKind == reflect.Slice {
			destValue = reflect.New(destValue.Type().Elem()).Elem()
			destType = destValue.Type()
			destValueKind = destValue.Kind()
			continue
		}
		break
	}

	var numField = destType.NumField()
	result = make([]string, 0, numField)
	for i := 0; i < numField; i++ {
		var filedStruct = destType.Field(i)
		var tag = filedStruct.Tag.Get(k_SQL_TAG)
		if tag != "" && tag != k_SQL_NO_TAG {
			result = append(result, tag)
		}
	}
	if len(result) > 0 {
		tagMap.Store(key, result)
	}
	return result, err
}

type FindResult struct {
	s   Executor
	sb  *SelectBuilder
	err error
}

func (this *FindResult) Total() (result int64, err error) {
	if this.err != nil {
		return 0, this.err
	}
	err = this.sb.Count().ScanRow(this.s, &result)
	return result, err
}

// --------------------------------------------------------------------------------
func Find(s Executor, table string, dest interface{}, limit, offset int64, w Statement) (result *FindResult, err error) {
	fieldList, err := getTagList(dest)
	var sb = NewSelectBuilder()
	sb.Selects(fieldList...)
	sb.From(table)
	if w != nil {
		sb.Where(w)
	}
	if limit > 0 {
		sb.Limit(limit)
	}
	if offset >= 0 {
		sb.Offset(offset)
	}
	if err = sb.Scan(s, dest); err != nil {
		return nil, err
	}
	result = &FindResult{s: s, sb: sb, err: err}
	return result, err
}

func FindAll(s Executor, table string, dest interface{}) (result *FindResult, err error) {
	return Find(s, table, dest, -1, -1, nil)
}

func FindOne(s Executor, table string, dest interface{}, w Statement) (err error) {
	_, err = Find(s, table, dest, 1, -1, w)
	return err
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
