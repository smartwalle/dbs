package dbs

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"time"
)

const (
	kNoTag = "-"
	kTag   = "sql"
)

type Scanner interface {
	Scan(rows *sql.Rows, dest interface{}) error
}

type scanner struct {
	tag     string
	structs atomic.Value // map[reflect.Type]structMetadata
	mu      *sync.Mutex
}

func NewScanner(tag string) *scanner {
	var m = &scanner{}
	m.tag = tag
	m.structs.Store(make(map[reflect.Type]structMetadata))
	m.mu = &sync.Mutex{}
	return m
}

func (s *scanner) Scan(rows *sql.Rows, dest interface{}) error {
	if rows == nil {
		return sql.ErrNoRows
	}

	if err := rows.Err(); err != nil {
		return err
	}

	columns, err := rows.ColumnTypes()
	if err != nil {
		return err
	}

	var destValue = reflect.ValueOf(dest)
	var destType = destValue.Type()

	if destValue.Kind() != reflect.Ptr {
		return errors.New("must pass a pointer")
	}

	if destValue.IsNil() {
		return errors.New("nil pointer passed")
	}

	var realType = destType
	for realType.Kind() == reflect.Ptr {
		realType = realType.Elem()
	}

	var isSlice = realType.Kind() == reflect.Slice
	if isSlice {
		return s.scanSlice(rows, columns, destType, destValue)
	}
	return s.scanOne(rows, columns, destType, destValue)
}

func (s *scanner) prepare(destType reflect.Type, columns []*sql.ColumnType) (fields []*fieldMetadata, values []interface{}) {
	var mStruct, ok = s.getStructMetadata(destType)
	if !ok {
		mStruct = s.buildStructMetadata(destType)
	}
	fields = make([]*fieldMetadata, len(columns))
	values = make([]interface{}, len(columns))
	for idx, column := range columns {
		var field = mStruct.Field(column.Name())
		if field != nil {
			fields[idx] = field
			values[idx] = field.TypePool.Get()
		} else {
			var val interface{}
			values[idx] = &val
		}
	}
	return fields, values
}

func (s *scanner) scanOne(rows *sql.Rows, columns []*sql.ColumnType, destType reflect.Type, destValue reflect.Value) error {
	if !rows.Next() {
		return sql.ErrNoRows
	}

	destType, destValue = base(destType, destValue)

	switch destType.Kind() {
	case reflect.Struct:
		var fields, values = s.prepare(destType, columns)
		defer func() {
			for idx, value := range values {
				var field = fields[idx]
				if field != nil {
					fields[idx].TypePool.Put(value)
				}
			}
		}()

		return s.scanIntoStruct(rows, columns, fields, values, destValue)
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
	default:
		return fmt.Errorf("%s is unsupported", destType.Kind())
	}
	return nil
}

func (s *scanner) scanSlice(rows *sql.Rows, columns []*sql.ColumnType, destType reflect.Type, destValue reflect.Value) error {
	destType, destValue = base(destType, destValue)

	// 获取 slice 元素类型
	destType = destType.Elem()

	// 判断元素类型是否为指针
	var isPointer = destType.Kind() == reflect.Ptr

	if isPointer {
		destType = destType.Elem()
	}

	switch destType.Kind() {
	case reflect.Struct:
		var fields, values = s.prepare(destType, columns)
		defer func() {
			for idx, value := range values {
				var field = fields[idx]
				if field != nil {
					fields[idx].TypePool.Put(value)
				}
			}
		}()

		var nList = make([]reflect.Value, 0, 20)
		for rows.Next() {
			var nPointer = reflect.New(destType)
			var nValue = reflect.Indirect(nPointer)

			if err := s.scanIntoStruct(rows, columns, fields, values, nValue); err != nil {
				return err
			}

			if isPointer {
				nList = append(nList, nPointer)
			} else {
				nList = append(nList, nValue)
			}
		}

		if len(nList) > 0 {
			destValue.Set(reflect.Append(destValue, nList...))
		}
	case reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64,
		reflect.String:
		var nList = make([]reflect.Value, 0, 20)
		for rows.Next() {
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
		}
		if len(nList) > 0 {
			destValue.Set(reflect.Append(destValue, nList...))
		}
	default:
		return fmt.Errorf("%s is unsupported", destType.Kind())
	}
	return nil
}

func (s *scanner) scanIntoStruct(rows *sql.Rows, columns []*sql.ColumnType, fields []*fieldMetadata, values []interface{}, destValue reflect.Value) error {
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
			}
		} else if len(columns) == 1 && !isScanner {
			var value = reflect.ValueOf(values[idx]).Elem().Elem()
			if value.IsValid() {
				destValue.Set(value)
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

func (s *scanner) getStructMetadata(key reflect.Type) (structMetadata, bool) {
	var value, exists = s.structs.Load().(map[reflect.Type]structMetadata)[key]
	return value, exists
}

func (s *scanner) setStructMetadata(key reflect.Type, value structMetadata) {
	var structs = s.structs.Load().(map[reflect.Type]structMetadata)
	var nStructs = make(map[reflect.Type]structMetadata, len(structs)+1)
	for k, v := range structs {
		nStructs[k] = v
	}
	nStructs[key] = value
	s.structs.Store(nStructs)
}

type element struct {
	Type  reflect.Type
	Index []int
}

func (s *scanner) buildStructMetadata(destType reflect.Type) structMetadata {
	s.mu.Lock()

	var mStruct, exists = s.getStructMetadata(destType)
	if exists {
		s.mu.Unlock()
		return mStruct
	}

	var queue = make([]element, 0, 10)
	queue = append(queue, element{
		Type:  destType,
		Index: nil,
	})

	var fields = make(map[string]*fieldMetadata)

	for len(queue) > 0 {
		var current = queue[0]
		queue = queue[1:]

		var numField = current.Type.NumField()

		for i := 0; i < numField; i++ {
			var fieldStruct = current.Type.Field(i)

			var tag = fieldStruct.Tag.Get(s.tag)
			if tag == kNoTag {
				continue
			}

			if tag == "" {
				tag = fieldStruct.Name

				if fieldStruct.Type.Kind() == reflect.Ptr {
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
			}

			if _, exists = fields[tag]; exists {
				continue
			}

			var field = &fieldMetadata{}
			field.Index = append(current.Index, i)
			field.Type = fieldStruct.Type
			field.TypePool = getTypePool(field.Type)
			fields[tag] = field
		}
	}
	mStruct.fields = fields

	s.setStructMetadata(destType, mStruct)
	s.mu.Unlock()

	return mStruct
}

type fieldMetadata struct {
	Index    []int
	Type     reflect.Type
	TypePool *sync.Pool
}

type structMetadata struct {
	fields map[string]*fieldMetadata
}

func (s structMetadata) Field(name string) *fieldMetadata {
	var field = s.fields[name]
	return field
}

var typePool = sync.Map{}

func getTypePool(reflectType reflect.Type) *sync.Pool {
	var pool, _ = typePool.LoadOrStore(reflectType, &sync.Pool{
		New: func() interface{} {
			return reflect.New(reflect.PointerTo(reflectType)).Interface()
		},
	})
	return pool.(*sync.Pool)
}
