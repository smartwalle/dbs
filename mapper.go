package dbs

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	kTagDisable   = "-"
	kTagSQL       = "sql"
	kTagAuto      = "auto"
	kTagSeparator = ";"
)

var ErrInvalidEncodeValue = errors.New("dbs: encode value must be a non-nil struct or struct pointer")

type Mapper interface {
	Encode(src any) (values []FieldValue, err error)

	Decode(rows *sql.Rows, dest any) (int, error)
}

type mapper struct {
	tag     string
	structs atomic.Value // map[reflect.Type]structMetadata
	mu      *sync.Mutex
}

func NewMapper(tag string) *mapper {
	var m = &mapper{}
	m.tag = tag
	m.structs.Store(make(map[reflect.Type]structMetadata))
	m.mu = &sync.Mutex{}
	return m
}

func (m *mapper) Encode(src any) (values []FieldValue, err error) {
	var srcType, srcValue, nErr = encodeBase(src)
	if nErr != nil {
		return nil, nErr
	}

	var mStruct, ok = m.getStructMetadata(srcType)
	if !ok {
		mStruct = m.buildStructMetadata(srcType)
	}

	values = make([]FieldValue, 0, len(mStruct.fields))
	for _, column := range mStruct.columns {
		var field = mStruct.fields[column]
		if field == nil {
			continue
		}

		var value, found = findFieldByIndex(srcValue, field.Index)
		if !found {
			continue
		}
		if field.Auto && value.IsZero() {
			continue
		}
		values = append(values, FieldValue{Name: column, Value: value.Interface()})
	}
	return values, nil
}

func encodeBase(src any) (reflect.Type, reflect.Value, error) {
	if src == nil {
		return nil, reflect.Value{}, ErrInvalidEncodeValue
	}

	var srcValue = reflect.ValueOf(src)
	var srcType = srcValue.Type()

	for srcType.Kind() == reflect.Ptr {
		if srcValue.IsNil() {
			return nil, reflect.Value{}, ErrInvalidEncodeValue
		}
		srcValue = srcValue.Elem()
		srcType = srcType.Elem()
	}

	if srcType.Kind() != reflect.Struct {
		return nil, reflect.Value{}, ErrInvalidEncodeValue
	}

	return srcType, srcValue, nil
}

func (m *mapper) Decode(rows *sql.Rows, dest any) (rowsAffected int, err error) {
	if rows == nil {
		return rowsAffected, sql.ErrNoRows
	}

	if dest == nil {
		return rowsAffected, errors.New("nil pointer passed")
	}

	var destValue = reflect.ValueOf(dest)
	var destType = destValue.Type()

	if destValue.Kind() != reflect.Ptr {
		return rowsAffected, errors.New("must pass a pointer")
	}

	if destValue.IsNil() {
		return rowsAffected, errors.New("nil pointer passed")
	}

	if err = rows.Err(); err != nil {
		return rowsAffected, err
	}

	columns, err := rows.ColumnTypes()
	if err != nil {
		return rowsAffected, err
	}

	if !rows.Next() {
		if err = rows.Err(); err != nil {
			return rowsAffected, err
		}
		return rowsAffected, sql.ErrNoRows
	}

	destType, destValue = base(destType, destValue)

	if destType.Kind() == reflect.Slice {
		var oldLen = destValue.Len()
		if err = m.scanSlice(rows, columns, dest, destType, destValue); err != nil {
			return 0, err
		}
		rowsAffected = destValue.Len() - oldLen
	} else {
		if err = m.scanOne(rows, columns, dest, destType, destValue); err != nil {
			return 0, err
		}
		rowsAffected = 1
	}
	return rowsAffected, nil
}

func (m *mapper) prepare(destType reflect.Type, columns []*sql.ColumnType) (fields []*fieldMetadata, values []any) {
	var mStruct, ok = m.getStructMetadata(destType)
	if !ok {
		mStruct = m.buildStructMetadata(destType)
	}
	fields = make([]*fieldMetadata, len(columns))
	values = make([]any, len(columns))
	for idx, column := range columns {
		var field = mStruct.Field(column.Name())
		if field != nil {
			fields[idx] = field
			values[idx] = field.ValuePool.Get()
		} else {
			var val any
			values[idx] = &val
		}
	}
	return fields, values
}

func (m *mapper) scanOne(rows *sql.Rows, columns []*sql.ColumnType, dest any, destType reflect.Type, destValue reflect.Value) error {
	switch destType.Kind() {
	case reflect.Struct:
		var fields, values = m.prepare(destType, columns)
		defer func() {
			for idx, value := range values {
				var field = fields[idx]
				if field != nil {
					fields[idx].ValuePool.Put(value)
				}
			}
		}()

		return scanIntoStruct(rows, columns, fields, values, destValue)
	case reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64,
		reflect.String:
		var nPointer = reflect.New(reflect.PointerTo(destType))
		if err := rows.Scan(nPointer.Interface()); err != nil {
			return err
		}
		var nValue = nPointer.Elem().Elem()
		if nValue.IsValid() {
			destValue.Set(nValue)
		}
	case reflect.Map:
		if destType.Key().Kind() == reflect.String {
			switch destType.Elem().Kind() {
			case reflect.Bool:
				return scanIntoMap[bool](rows, false, columns, dest)
			case reflect.Int:
				return scanIntoMap[int](rows, false, columns, dest)
			case reflect.Int8:
				return scanIntoMap[int8](rows, false, columns, dest)
			case reflect.Int16:
				return scanIntoMap[int16](rows, false, columns, dest)
			case reflect.Int32:
				return scanIntoMap[int32](rows, false, columns, dest)
			case reflect.Int64:
				return scanIntoMap[int64](rows, false, columns, dest)
			case reflect.Uint:
				return scanIntoMap[uint](rows, false, columns, dest)
			case reflect.Uint8:
				return scanIntoMap[uint8](rows, false, columns, dest)
			case reflect.Uint16:
				return scanIntoMap[uint16](rows, false, columns, dest)
			case reflect.Uint32:
				return scanIntoMap[uint32](rows, false, columns, dest)
			case reflect.Uint64:
				return scanIntoMap[uint64](rows, false, columns, dest)
			case reflect.Float32:
				return scanIntoMap[float32](rows, false, columns, dest)
			case reflect.Float64:
				return scanIntoMap[float64](rows, false, columns, dest)
			case reflect.String:
				return scanIntoMap[string](rows, false, columns, dest)
			case reflect.Interface:
				return scanIntoMap[any](rows, true, columns, dest)
			}
		}
		return fmt.Errorf("map[%s]%s is unsupported", destType.Key().Kind(), destType.Elem().Kind())
	default:
		return fmt.Errorf("%s is unsupported", destType.Kind())
	}
	return nil
}

func (m *mapper) scanSlice(rows *sql.Rows, columns []*sql.ColumnType, dest any, destType reflect.Type, destValue reflect.Value) error {
	// 获取 slice 元素类型
	destType = destType.Elem()

	// 判断元素类型是否为指针
	var isPointer = destType.Kind() == reflect.Ptr

	if isPointer {
		destType = destType.Elem()
	}

	switch destType.Kind() {
	case reflect.Struct:
		var fields, values = m.prepare(destType, columns)
		defer func() {
			for idx, value := range values {
				var field = fields[idx]
				if field != nil {
					fields[idx].ValuePool.Put(value)
				}
			}
		}()

		var nList = make([]reflect.Value, 0, 20)
		for {
			var nPointer = reflect.New(destType)
			var nValue = reflect.Indirect(nPointer)

			if err := scanIntoStruct(rows, columns, fields, values, nValue); err != nil {
				return err
			}

			if isPointer {
				nList = append(nList, nPointer)
			} else {
				nList = append(nList, nValue)
			}

			if !rows.Next() {
				break
			}
		}
		if err := rows.Err(); err != nil {
			return err
		}

		if len(nList) > 0 {
			destValue.Set(reflect.Append(destValue, nList...))
		}
	case reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64,
		reflect.String:
		var nList = make([]reflect.Value, 0, 20)
		for {
			var nPointer = reflect.New(reflect.PointerTo(destType))
			if err := rows.Scan(nPointer.Interface()); err != nil {
				return err
			}
			var nValue = nPointer.Elem().Elem()
			if nValue.IsValid() {
				if isPointer {
					nList = append(nList, nPointer.Elem())
				} else {
					nList = append(nList, nValue)
				}
			}

			if !rows.Next() {
				break
			}
		}
		if err := rows.Err(); err != nil {
			return err
		}
		if len(nList) > 0 {
			destValue.Set(reflect.Append(destValue, nList...))
		}
	case reflect.Map:
		if destType.Key().Kind() == reflect.String {
			switch destType.Elem().Kind() {
			case reflect.Bool:
				return scanIntoMaps[bool](rows, false, columns, dest)
			case reflect.Int:
				return scanIntoMaps[int](rows, false, columns, dest)
			case reflect.Int8:
				return scanIntoMaps[int8](rows, false, columns, dest)
			case reflect.Int16:
				return scanIntoMaps[int16](rows, false, columns, dest)
			case reflect.Int32:
				return scanIntoMaps[int32](rows, false, columns, dest)
			case reflect.Int64:
				return scanIntoMaps[int64](rows, false, columns, dest)
			case reflect.Uint:
				return scanIntoMaps[uint](rows, false, columns, dest)
			case reflect.Uint8:
				return scanIntoMaps[uint8](rows, false, columns, dest)
			case reflect.Uint16:
				return scanIntoMaps[uint16](rows, false, columns, dest)
			case reflect.Uint32:
				return scanIntoMaps[uint32](rows, false, columns, dest)
			case reflect.Uint64:
				return scanIntoMaps[uint64](rows, false, columns, dest)
			case reflect.Float32:
				return scanIntoMaps[float32](rows, false, columns, dest)
			case reflect.Float64:
				return scanIntoMaps[float64](rows, false, columns, dest)
			case reflect.String:
				return scanIntoMaps[string](rows, false, columns, dest)
			case reflect.Interface:
				return scanIntoMaps[any](rows, true, columns, dest)
			}
		}
		return fmt.Errorf("[]map[%s]%s is unsupported", destType.Key().Kind(), destType.Elem().Kind())
	default:
		return fmt.Errorf("%s is unsupported", destType.Kind())
	}
	return nil
}

func scanIntoMap[T any](rows *sql.Rows, useColumnType bool, columns []*sql.ColumnType, dest any) error {
	var mapValue, ok = dest.(*map[string]T)
	if !ok {
		return fmt.Errorf("%+v is unsupported", dest)
	}
	if *mapValue == nil {
		*mapValue = map[string]T{}
	}
	return scanIntoMapValue[T](rows, useColumnType, columns, *mapValue)
}

func scanIntoMaps[T any](rows *sql.Rows, useColumnType bool, columns []*sql.ColumnType, dest any) error {
	var mapValues, ok = dest.(*[]map[string]T)
	if !ok {
		return fmt.Errorf("%+v is unsupported", dest)
	}

	if *mapValues == nil {
		*mapValues = []map[string]T{}
	}

	for {
		var mapValue = make(map[string]T)
		if err := scanIntoMapValue[T](rows, useColumnType, columns, mapValue); err != nil {
			return err
		}
		*mapValues = append(*mapValues, mapValue)

		if !rows.Next() {
			break
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	return nil
}

func scanIntoMapValue[T any](rows *sql.Rows, useColumnType bool, columns []*sql.ColumnType, mapValue map[string]T) (err error) {
	var values = make([]any, len(columns))
	for idx, column := range columns {
		if useColumnType && column.ScanType() != nil {
			values[idx] = reflect.New(reflect.PointerTo(column.ScanType())).Interface()
		} else {
			var val T
			values[idx] = &val
		}
	}

	if err = rows.Scan(values...); err != nil {
		return err
	}

	var zeroValue T
	for idx, column := range columns {
		var name = column.Name()
		if reflectValue := reflect.Indirect(reflect.Indirect(reflect.ValueOf(values[idx]))); reflectValue.IsValid() {
			var value = reflectValue.Interface()
			if valuer, ok := value.(driver.Valuer); ok {
				value, err = valuer.Value()
				if err != nil {
					return err
				}
			} else if b, ok := value.(sql.RawBytes); ok {
				value = string(b)
			}
			var nValue, ok = value.(T)
			if !ok {
				return fmt.Errorf("dbs: cannot assign column %q value of type %T to map value", name, value)
			}
			mapValue[name] = nValue
		} else {
			mapValue[name] = zeroValue
		}
	}
	return nil
}

func scanIntoStruct(rows *sql.Rows, columns []*sql.ColumnType, fields []*fieldMetadata, values []any, destValue reflect.Value) error {
	var isScanner = false
	if len(columns) == 1 && fields[0] == nil {
		switch destValue.Addr().Interface().(type) {
		case time.Time:
			var val time.Time
			values[0] = &val
		case *time.Time:
			var val *time.Time
			values[0] = &val
		case sql.Scanner:
			values[0] = destValue.Addr().Interface()
			isScanner = true
		}
	}

	if err := rows.Scan(values...); err != nil {
		return err
	}

	for idx := range columns {
		var field = fields[idx]
		if field != nil {
			var value = reflect.ValueOf(values[idx]).Elem().Elem()
			if value.IsValid() {
				fieldByIndex(destValue, fields[idx].Index).Set(value)
			} else {
				fieldByIndex(destValue, fields[idx].Index).Set(reflect.Zero(fields[idx].Type))
			}
		} else if len(columns) == 1 && !isScanner {
			var value = reflect.ValueOf(values[idx]).Elem().Elem()
			if !value.IsValid() {
				destValue.Set(reflect.Zero(destValue.Type()))
			} else if value.Type().AssignableTo(destValue.Type()) {
				destValue.Set(value)
			} else if value.Type().ConvertibleTo(destValue.Type()) {
				destValue.Set(value.Convert(destValue.Type()))
			} else {
				return fmt.Errorf("dbs: cannot assign column %q value of type %s to %s", columns[idx].Name(), value.Type(), destValue.Type())
			}
		}
	}
	return nil
}

func base(destType reflect.Type, destValue reflect.Value) (reflect.Type, reflect.Value) {
	for {
		if destValue.Kind() == reflect.Ptr && destValue.IsNil() {
			destValue.Set(reflect.New(destType.Elem()))
		}

		if destValue.Kind() == reflect.Ptr {
			destValue = destValue.Elem()
			destType = destType.Elem()
			continue
		}
		break
	}
	return destType, destValue
}

func findFieldByIndex(value reflect.Value, index []int) (reflect.Value, bool) {
	for i, x := range index {
		if i > 0 && value.Kind() == reflect.Pointer && value.Type().Elem().Kind() == reflect.Struct {
			if value.IsNil() {
				return reflect.Value{}, false
			}
			value = value.Elem()
		}
		value = value.Field(x)
	}
	return value, true
}

func fieldByIndex(value reflect.Value, index []int) reflect.Value {
	if len(index) == 1 {
		return value.Field(index[0])
	}
	for i, x := range index {
		if i > 0 {
			if value.Kind() == reflect.Pointer && value.Type().Elem().Kind() == reflect.Struct {
				if value.IsNil() {
					value.Set(reflect.New(value.Type().Elem()))
				}
				value = value.Elem()
			}
		}
		value = value.Field(x)
	}
	return value
}

func (m *mapper) getStructMetadata(key reflect.Type) (structMetadata, bool) {
	var value, ok = m.structs.Load().(map[reflect.Type]structMetadata)[key]
	return value, ok
}

func (m *mapper) setStructMetadata(key reflect.Type, value structMetadata) {
	var structs = m.structs.Load().(map[reflect.Type]structMetadata)
	var nStructs = make(map[reflect.Type]structMetadata, len(structs)+1)
	for k, v := range structs {
		nStructs[k] = v
	}
	nStructs[key] = value
	m.structs.Store(nStructs)
}

type element struct {
	Type  reflect.Type
	Index []int
}

func (m *mapper) buildStructMetadata(destType reflect.Type) structMetadata {
	m.mu.Lock()

	var mStruct, ok = m.getStructMetadata(destType)
	if ok {
		m.mu.Unlock()
		return mStruct
	}

	var queue = make([]element, 0, 10)
	queue = append(queue, element{
		Type:  destType,
		Index: nil,
	})

	var fields = make(map[string]*fieldMetadata)
	var columns = make([]string, 0)

	for len(queue) > 0 {
		var current = queue[0]
		queue = queue[1:]

		var numField = current.Type.NumField()

		for i := 0; i < numField; i++ {
			var fieldStruct = current.Type.Field(i)
			if fieldStruct.PkgPath != "" {
				continue
			}

			var tag = fieldStruct.Tag.Get(m.tag)
			if tag == kTagDisable {
				continue
			}

			if tag == "" {
				if fieldStruct.Type.Kind() == reflect.Ptr && fieldStruct.Type.Elem().Kind() == reflect.Struct {
					queue = append(queue, element{
						Type:  fieldStruct.Type.Elem(),
						Index: append(current.Index, i),
					})
					continue
				}

				if fieldStruct.Type.Kind() == reflect.Struct {
					queue = append(queue, element{
						Type:  fieldStruct.Type,
						Index: append(current.Index, i),
					})
					continue
				}
				continue
			}

			var tagValues = strings.Split(tag, kTagSeparator)
			var fieldName = strings.TrimSpace(tagValues[0])
			if fieldName == "" {
				continue
			}
			if _, ok = fields[fieldName]; ok {
				continue
			}

			var tagMap = make(map[string]bool)
			for _, tagValue := range tagValues {
				tagMap[strings.ToLower(strings.TrimSpace(tagValue))] = true
			}

			var field = &fieldMetadata{}
			field.Index = append(current.Index, i)
			field.Type = fieldStruct.Type
			field.ValuePool = getValuePool(field.Type)
			field.Auto = tagMap[kTagAuto]
			fields[fieldName] = field
			columns = append(columns, fieldName)
		}
	}
	mStruct.columns = columns
	mStruct.fields = fields

	m.setStructMetadata(destType, mStruct)
	m.mu.Unlock()

	return mStruct
}

type fieldMetadata struct {
	Index     []int
	Type      reflect.Type
	ValuePool *sync.Pool
	Auto      bool
}

type structMetadata struct {
	columns []string
	fields  map[string]*fieldMetadata
}

func (s structMetadata) Field(name string) *fieldMetadata {
	var field = s.fields[name]
	return field
}

var cachedValue = sync.Map{}

func getValuePool(reflectType reflect.Type) *sync.Pool {
	var pool, _ = cachedValue.LoadOrStore(reflectType, &sync.Pool{
		New: func() interface{} {
			return reflect.New(reflect.PointerTo(reflectType)).Interface()
		},
	})
	return pool.(*sync.Pool)
}

type FieldValue struct {
	Name  string
	Value any
}
