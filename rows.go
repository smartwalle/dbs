package dba

import (
	"database/sql"
	"errors"
	"reflect"
)

const (
	k_SQL_TAG    = "sql"
	k_SQL_NO_TAG = "-"
)

func Scan(rows *sql.Rows, result interface{}) (err error) {
	if rows == nil {
		return errors.New("sql: rows is closed")
	}
	defer rows.Close()

	var objType = reflect.TypeOf(result)
	var objValue = reflect.ValueOf(result)
	var objValueKind = objValue.Kind()

	if objValueKind == reflect.Struct {
		return errors.New("result argument is struct")
	}

	if objValue.IsNil() {
		return errors.New("result argument is nil")
	}

	for {
		if objValueKind == reflect.Ptr && objValue.IsNil() {
			objValue.Set(reflect.New(objType.Elem()))
		}

		if objValueKind == reflect.Ptr {
			objValue = objValue.Elem()
			objType = objType.Elem()
			objValueKind = objValue.Kind()
			continue
		}
		break
	}

	// 获取查询的列
	columns, err := rows.Columns()
	if err != nil {
		return err
	}

	if objValueKind == reflect.Slice {
		if objValue.IsValid() {
			objValue.Set(reflect.MakeSlice(objType, 0, 0))
		}
		var sliceValue = objValue

		var hasData = false
		for rows.Next() {
			var obj = reflect.New(sliceValue.Type().Elem())

			err = _scan(rows, columns, obj.Interface())
			if err != nil {
				return err
			}
			hasData = true
			sliceValue = reflect.Append(sliceValue, obj.Elem())
		}
		if hasData {
			objValue.Set(sliceValue)
		} else {
			return errors.New("sql: no rows in result set")
		}

	} else {
		for rows.Next() {
			return _scan(rows, columns, result)
		}
		return errors.New("sql: no rows in result set")
	}

	return err
}

func _scan(rows *sql.Rows, columns []string, result interface{}) (err error) {
	var objType = reflect.TypeOf(result)
	var objValue = reflect.ValueOf(result)
	var objValueKind = objValue.Kind()

	if objValueKind == reflect.Struct {
		return errors.New("result argument is struct")
	}

	if objValue.IsNil() {
		return errors.New("result argument is nil")
	}

	for {
		if objValueKind == reflect.Ptr && objValue.IsNil() {
			objValue.Set(reflect.New(objType.Elem()))
		}

		if objValueKind == reflect.Ptr {
			objValue = objValue.Elem()
			objType = objType.Elem()
			objValueKind = objValue.Kind()
			continue
		}
		break
	}

	var fields = make(map[string]*field)
	getFields(fields, objType, objValue)

	var valueList = make([]interface{}, 0, len(columns))
	var selectedFields = make([]*field, 0, len(columns))

	for _, column := range columns {
		if f, ok := fields[column]; ok {
			if f.Value.Kind() == reflect.Ptr {
				valueList = append(valueList, f.Value.Addr().Interface())
			} else {
				var value = reflect.New(reflect.PtrTo(f.Struct.Type))
				value.Elem().Set(f.Value.Addr())

				valueList = append(valueList, value.Interface())
			}
			selectedFields = append(selectedFields, f)
		}
	}

	rows.Scan(valueList...)

	for index, f := range selectedFields {
		var v = reflect.ValueOf(valueList[index]).Elem().Elem()
		if v.IsValid() {
			f.Value.Set(v)
		}
	}

	return err
}

func getFields(fields map[string]*field, objType reflect.Type, objValue reflect.Value) {
	var numField = objType.NumField()

	for i := 0; i < numField; i++ {
		var fieldStruct = objType.Field(i)
		var fieldValue = objValue.Field(i)

		var tag = fieldStruct.Tag.Get(k_SQL_TAG)

		if tag == k_SQL_NO_TAG {
			continue
		}

		if tag == "" {
			if fieldValue.Kind() == reflect.Ptr {
				if fieldValue.IsNil() {
					fieldValue.Set(reflect.New(fieldValue.Type().Elem()))
				}
				fieldValue = fieldValue.Elem()
			}

			if fieldValue.Kind() == reflect.Struct {
				getFields(fields, fieldValue.Type(), fieldValue)
				continue
			}

			tag = fieldStruct.Name
		}

		var f = &field{}
		f.Value = fieldValue
		f.Struct = fieldStruct
		f.Name = tag

		fields[tag] = f
	}
}

type field struct {
	Name   string
	Struct reflect.StructField
	Value  reflect.Value
}
