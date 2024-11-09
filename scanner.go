package dbs

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
)

const (
	kNoTag = "-"
	kTag   = "sql"
)

type fieldDescriptor struct {
	Index []int
}

type structDescriptor struct {
	mu            *sync.Mutex
	Fields        map[string]fieldDescriptor
	UnknownFields map[string]reflect.Value
}

func (s structDescriptor) Field(parent reflect.Value, columnType *sql.ColumnType) reflect.Value {
	var columnName = columnType.Name()

	field, found := s.Fields[columnName]
	if found {
		return fieldByIndex(parent, field.Index).Addr()
	}

	value, found := s.UnknownFields[columnName]
	if !found {
		s.mu.Lock()
		value, found = s.UnknownFields[columnName]
		if !found {
			value = reflect.New(columnType.ScanType())
			s.UnknownFields[columnName] = value
		}
		s.mu.Unlock()
	}
	return value
}

type Scanner interface {
	Scan(rows *sql.Rows, dst interface{}) error
}

type scanner struct {
	tag     string
	structs atomic.Value // map[reflect.Type]structDescriptor
	mu      sync.Mutex
}

func NewScanner(tag string) *scanner {
	var m = &scanner{}
	m.tag = tag
	m.structs.Store(make(map[reflect.Type]structDescriptor))
	return m
}

func (s *scanner) Scan(rows *sql.Rows, dst interface{}) error {
	if rows == nil {
		return sql.ErrNoRows
	}

	if err := rows.Err(); err != nil {
		return err
	}

	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return err
	}

	var dstValue = reflect.ValueOf(dst)
	var dstType = dstValue.Type()

	if dstValue.Kind() != reflect.Ptr {
		return errors.New("must pass a pointer")
	}

	if dstValue.IsNil() {
		return errors.New("nil pointer passed")
	}

	var isSlice bool
	if dstType.Kind() == reflect.Ptr {
		isSlice = dstType.Elem().Kind() == reflect.Slice
	} else {
		isSlice = dstType.Kind() == reflect.Slice
	}

	if isSlice {
		return s.scanSlice(rows, columnTypes, dstType, dstValue)
	}
	return s.scanOne(rows, columnTypes, dstType, dstValue)
}

func (s *scanner) scanOne(rows *sql.Rows, columnTypes []*sql.ColumnType, dstType reflect.Type, dstValue reflect.Value) error {
	if !rows.Next() {
		return sql.ErrNoRows
	}

	dstType, dstValue = base(dstType, dstValue)

	switch dstType.Kind() {
	case reflect.Struct:
		var dStruct, ok = s.getStructDescriptor(dstType)
		if !ok {
			dStruct = s.parseStructDescriptor(dstType)
		}
		return s.scanIntoStruct(rows, columnTypes, dStruct, dstValue)
	case reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64,
		reflect.String:
		return rows.Scan(dstValue.Addr().Interface())
	default:
		return fmt.Errorf("%s is unsupported", dstType.Kind())
	}
	return nil
}

func (s *scanner) scanSlice(rows *sql.Rows, columnTypes []*sql.ColumnType, dstType reflect.Type, dstValue reflect.Value) error {
	dstType, dstValue = base(dstType, dstValue)

	// 获取 slice 元素类型
	dstType = dstType.Elem()

	// 判断元素类型是否为指针
	var isPointer = dstType.Kind() == reflect.Ptr

	if isPointer {
		dstType = dstType.Elem()
	}

	switch dstType.Kind() {
	case reflect.Struct:
		var dStruct, ok = s.getStructDescriptor(dstType)
		if !ok {
			dStruct = s.parseStructDescriptor(dstType)
		}

		var nList = make([]reflect.Value, 0, 20)
		for rows.Next() {
			var nPointer = reflect.New(dstType)
			var nValue = reflect.Indirect(nPointer)

			if err := s.scanIntoStruct(rows, columnTypes, dStruct, nValue); err != nil {
				return err
			}

			if isPointer {
				nList = append(nList, nPointer)
			} else {
				nList = append(nList, nValue)
			}
		}
		if len(nList) > 0 {
			dstValue.Set(reflect.Append(dstValue, nList...))
		}
	case reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64,
		reflect.String:
		var nList = make([]reflect.Value, 0, 20)
		for rows.Next() {
			var nPointer = reflect.New(dstType)
			var nValue = reflect.Indirect(nPointer)

			if err := rows.Scan(nPointer.Interface()); err != nil {
				return err
			}

			if isPointer {
				nList = append(nList, nPointer)
			} else {
				nList = append(nList, nValue)
			}
		}
		if len(nList) > 0 {
			dstValue.Set(reflect.Append(dstValue, nList...))
		}
	default:
		return fmt.Errorf("%s is unsupported", dstType.Kind())
	}
	return nil
}

func (s *scanner) scanIntoStruct(rows *sql.Rows, columnTypes []*sql.ColumnType, dStruct structDescriptor, dstValue reflect.Value) error {
	var values = make([]interface{}, len(columnTypes))

	for idx, columnType := range columnTypes {
		values[idx] = reflect.New(dStruct.Field(dstValue, columnType).Type()).Interface()
	}

	if err := rows.Scan(values...); err != nil {
		return err
	}

	for idx, columnType := range columnTypes {
		var value = reflect.ValueOf(values[idx]).Elem().Elem()
		if value.IsValid() {
			dStruct.Field(dstValue, columnType).Elem().Set(value)
		}
	}
	return nil
}

func base(dstType reflect.Type, dstValue reflect.Value) (reflect.Type, reflect.Value) {
	for {
		if dstValue.Kind() == reflect.Ptr && dstValue.IsNil() {
			dstValue.Set(reflect.New(dstType.Elem()))
		}

		if dstValue.Kind() == reflect.Ptr {
			dstValue = dstValue.Elem()
			dstType = dstType.Elem()
			continue
		}
		break
	}
	return dstType, dstValue
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

func (s *scanner) getStructDescriptor(key reflect.Type) (structDescriptor, bool) {
	var value, ok = s.structs.Load().(map[reflect.Type]structDescriptor)[key]
	return value, ok
}

func (s *scanner) setStructDescriptor(key reflect.Type, value structDescriptor) {
	var structs = s.structs.Load().(map[reflect.Type]structDescriptor)
	var nStructs = make(map[reflect.Type]structDescriptor, len(structs)+1)
	for k, v := range structs {
		nStructs[k] = v
	}
	nStructs[key] = value
	s.structs.Store(nStructs)
}

type structQueueElement struct {
	Type  reflect.Type
	Index []int
}

func (s *scanner) parseStructDescriptor(dstType reflect.Type) structDescriptor {
	s.mu.Lock()

	var dStruct, ok = s.getStructDescriptor(dstType)
	if ok {
		s.mu.Unlock()
		return dStruct
	}

	var queue = make([]structQueueElement, 0, 3)
	queue = append(queue, structQueueElement{
		Type:  dstType,
		Index: nil,
	})

	var dFields = make(map[string]fieldDescriptor)

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
					queue = append(queue, structQueueElement{
						Type:  fieldStruct.Type.Elem(),
						Index: append(current.Index, i),
					})
					continue
				}

				if fieldStruct.Type.Kind() == reflect.Struct {
					queue = append(queue, structQueueElement{
						Type:  fieldStruct.Type,
						Index: append(current.Index, i),
					})
					continue
				}
			}

			if _, exists := dFields[tag]; exists {
				continue
			}

			var dField = fieldDescriptor{}
			dField.Index = append(current.Index, i)
			dFields[tag] = dField
		}
	}

	dStruct.mu = &sync.Mutex{}
	dStruct.Fields = dFields
	dStruct.UnknownFields = make(map[string]reflect.Value)

	s.setStructDescriptor(dstType, dStruct)
	s.mu.Unlock()

	return dStruct
}
