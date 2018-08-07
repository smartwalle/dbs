package dbs

import (
	"errors"
	"reflect"
	"sync"
)

var fieldMap = sync.Map{}

func GetFields(dest interface{}) (result []string, err error) {
	if dest == nil {
		return nil, errors.New("dest argument is nil")
	}

	var destType = reflect.TypeOf(dest)
	var destValue = reflect.ValueOf(dest)
	var destValueKind = destValue.Kind()

	var rawType = destType
	var rawValue = destValue

	var key = destType.String()
	if value, ok := fieldMap.Load(key); ok {
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
		var fieldStruct = destType.Field(i)
		var tag = fieldStruct.Tag.Get(k_SQL_TAG)
		if tag != "" && tag != k_SQL_NO_TAG {
			result = append(result, tag)
		}
	}
	if len(result) > 0 {
		fieldMap.Store(key, result)
	}

	rawValue.Elem().Set(reflect.Zero(rawType.Elem()))

	return result, err
}

// --------------------------------------------------------------------------------
func Find(s Executor, table string, dest interface{}, limit, offset int64, w Statement) (err error) {
	fieldList, err := GetFields(dest)
	if err != nil {
		return err
	}
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
		return err
	}
	return err
}

func FindAll(s Executor, table string, dest interface{}) (err error) {
	return Find(s, table, dest, -1, -1, nil)
}

func FindOne(s Executor, table string, dest interface{}, w Statement) (err error) {
	return Find(s, table, dest, 1, -1, w)
}