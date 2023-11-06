package dbs

import (
	"database/sql"
	"errors"
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
	Fields map[string]fieldDescriptor
}

type Mapper struct {
	tag     string
	structs atomic.Value // map[reflect.Type]structDescriptor
	mu      sync.Mutex
}

func NewMapper(tag string) *Mapper {
	var m = &Mapper{}
	m.tag = tag
	m.structs.Store(make(map[reflect.Type]structDescriptor))
	return m
}

func (mapper *Mapper) Decode(rows *sql.Rows, dst interface{}) error {
	if rows == nil {
		return sql.ErrNoRows
	}

	if err := rows.Err(); err != nil {
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
		return mapper.decodeSlice(rows, dstType, dstValue)
	}
	return mapper.decodeOne(rows, dstType, dstValue)
}

func (mapper *Mapper) decodeOne(rows *sql.Rows, dstType reflect.Type, dstValue reflect.Value) error {
	if !rows.Next() {
		return sql.ErrNoRows
	}

	columns, err := rows.Columns()
	if err != nil {
		return err
	}

	dstType, dstValue = base(dstType, dstValue)

	var dStruct, ok = mapper.getStructDescriptor(dstType)
	if !ok {
		dStruct = mapper.parseStructDescriptor(dstType)
	}

	var values = make([]interface{}, len(columns))
	for idx, column := range columns {
		values[idx] = fieldByIndex(dstValue, dStruct.Fields[column].Index).Addr().Interface()
	}
	return rows.Scan(values...)
}

func (mapper *Mapper) decodeSlice(rows *sql.Rows, dstType reflect.Type, dstValue reflect.Value) error {
	columns, err := rows.Columns()
	if err != nil {
		return err
	}

	dstType, dstValue = base(dstType, dstValue)

	// 获取 slice 元素类型
	dstType = dstType.Elem()

	// 判断元素类型是否为指针
	var isPointer = dstType.Kind() == reflect.Ptr

	if isPointer {
		dstType = dstType.Elem()
	}

	var dStruct, ok = mapper.getStructDescriptor(dstType)
	if !ok {
		dStruct = mapper.parseStructDescriptor(dstType)
	}

	var nColumns = make([]interface{}, len(columns))
	var nValues = make([]reflect.Value, 0, 20)
	for rows.Next() {
		var nPointer = reflect.New(dstType)
		var nValue = reflect.Indirect(nPointer)

		for idx, column := range columns {
			nColumns[idx] = fieldByIndex(nValue, dStruct.Fields[column].Index).Addr().Interface()
		}

		if err = rows.Scan(nColumns...); err != nil {
			return err
		}

		if isPointer {
			nValues = append(nValues, nPointer)
		} else {
			nValues = append(nValues, nValue)
		}
	}
	if len(nValues) > 0 {
		dstValue.Set(reflect.Append(dstValue, nValues...))
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

func fieldByIndex(parent reflect.Value, index []int) reflect.Value {
	if len(index) == 1 {
		return parent.Field(index[0])
	}
	for i, x := range index {
		if i > 0 {
			if parent.Kind() == reflect.Pointer && parent.Type().Elem().Kind() == reflect.Struct {
				if parent.IsNil() {
					parent.Set(reflect.New(parent.Type().Elem()))
				}
				parent = parent.Elem()
			}
		}
		parent = parent.Field(x)
	}
	return parent
}

func (mapper *Mapper) getStructDescriptor(key reflect.Type) (structDescriptor, bool) {
	var value, ok = mapper.structs.Load().(map[reflect.Type]structDescriptor)[key]
	return value, ok
}

func (mapper *Mapper) setStructDescriptor(key reflect.Type, value structDescriptor) {
	var structs = mapper.structs.Load().(map[reflect.Type]structDescriptor)
	var nStructs = make(map[reflect.Type]structDescriptor, len(structs)+1)
	for k, v := range structs {
		nStructs[k] = v
	}
	nStructs[key] = value
	mapper.structs.Store(nStructs)
}

type structQueueElement struct {
	Type  reflect.Type
	Index []int
}

func (mapper *Mapper) parseStructDescriptor(dstType reflect.Type) structDescriptor {
	mapper.mu.Lock()

	var dStruct, ok = mapper.getStructDescriptor(dstType)
	if ok {
		mapper.mu.Unlock()
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

			var tag = fieldStruct.Tag.Get(mapper.tag)
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

	dStruct.Fields = dFields

	mapper.setStructDescriptor(dstType, dStruct)
	mapper.mu.Unlock()

	return dStruct
}
