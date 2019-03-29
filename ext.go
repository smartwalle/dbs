package dbs

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"time"
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

	result = getField(destType, destValue)
	if len(result) > 0 {
		fieldMap.Store(key, result)
	}

	rawValue.Elem().Set(reflect.Zero(rawType.Elem()))

	return result, err
}

func getField(destType reflect.Type, destValue reflect.Value) (result []string) {
	var numField = destType.NumField()
	result = make([]string, 0, numField)
	for i := 0; i < numField; i++ {
		var fieldStruct = destType.Field(i)
		var tag = fieldStruct.Tag.Get(kSQLTag)
		if tag == kSQLNoTag {
			continue
		}

		if tag == "" {
			var fieldValue = destValue.Field(i)
			if fieldValue.Kind() == reflect.Ptr {
				if fieldValue.Type() == reflect.TypeOf(&time.Time{}) {
					continue
				}
				if fieldValue.IsNil() {
					fieldValue.Set(reflect.New(fieldValue.Type().Elem()))
				}
				fieldValue = fieldValue.Elem()
			}

			if fieldValue.Kind() == reflect.Struct {
				if fieldValue.Type() == reflect.TypeOf(time.Time{}) {
					continue
				}
				result = append(result, getField(fieldValue.Type(), fieldValue)...)
				continue
			}
		}

		if tag != "" {
			result = append(result, tag)
		}
	}
	return result
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
	if offset > 0 {
		sb.Offset(offset)
	}
	if err = sb.scanContext(context.Background(), s, dest); err != nil {
		return err
	}
	return err
}

func FindAll(s Executor, table string, dest interface{}, w Statement) (err error) {
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
	if err = sb.scanContext(context.Background(), s, dest); err != nil {
		return err
	}
	return err
}

func FindOne(s Executor, table string, dest interface{}, w Statement) (err error) {
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
	sb.Limit(1)
	if err = sb.scanContext(context.Background(), s, dest); err != nil {
		return err
	}
	return err
}
