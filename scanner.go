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

type Scanner interface {
	Scan(rows *sql.Rows, dst interface{}) error
}

type scanner struct {
	tag     string
	structs atomic.Value // map[reflect.Type]structMetadata
	mu      sync.Mutex
}

func NewScanner(tag string) *scanner {
	var m = &scanner{}
	m.tag = tag
	m.structs.Store(make(map[reflect.Type]structMetadata))
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

	var realType = dstType
	for realType.Kind() == reflect.Ptr {
		realType = realType.Elem()
	}

	var isSlice = realType.Kind() == reflect.Slice
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
		var mStruct, ok = s.getStructMetadata(dstType)
		if !ok {
			mStruct = s.parseStruct(dstType)
		}
		var fields = make([]*fieldMetadata, len(columnTypes))
		var values = make([]interface{}, len(columnTypes))
		for idx, columnType := range columnTypes {
			var field = mStruct.Field(columnType)
			fields[idx] = field
			values[idx] = field.TypePool.Get()
		}
		defer func() {
			for idx, value := range values {
				fields[idx].TypePool.Put(value)
			}
		}()

		return s.scanIntoStruct(rows, columnTypes, fields, values, dstValue)
	case reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64,
		reflect.String:
		var nPointer = reflect.New(reflect.PointerTo(dstType))
		if err := rows.Scan(nPointer.Interface()); err != nil {
			return err
		}
		var nValue = nPointer.Elem().Elem()
		if nValue.IsValid() {
			dstValue.Set(nValue)
		}
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
		var mStruct, ok = s.getStructMetadata(dstType)
		if !ok {
			mStruct = s.parseStruct(dstType)
		}

		var fields = make([]*fieldMetadata, len(columnTypes))
		var values = make([]interface{}, len(columnTypes))
		for idx, columnType := range columnTypes {
			var field = mStruct.Field(columnType)
			if field != nil {
				fields[idx] = field
				values[idx] = field.TypePool.Get()
			} else {
				var val interface{}
				values[idx] = &val
			}
		}
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
			var nPointer = reflect.New(dstType)
			var nValue = reflect.Indirect(nPointer)

			if err := s.scanIntoStruct(rows, columnTypes, fields, values, nValue); err != nil {
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
			var nPointer = reflect.New(reflect.PointerTo(dstType))
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
			dstValue.Set(reflect.Append(dstValue, nList...))
		}
	default:
		return fmt.Errorf("%s is unsupported", dstType.Kind())
	}
	return nil
}

func (s *scanner) scanIntoStruct(rows *sql.Rows, columnTypes []*sql.ColumnType, fields []*fieldMetadata, values []interface{}, dstValue reflect.Value) error {
	if err := rows.Scan(values...); err != nil {
		return err
	}

	for idx := range columnTypes {
		var field = fields[idx]
		if field != nil {
			var value = reflect.ValueOf(values[idx]).Elem().Elem()
			if value.IsValid() {
				fieldByIndex(dstValue, fields[idx].Index).Set(value)
			}
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

func (s *scanner) getStructMetadata(key reflect.Type) (structMetadata, bool) {
	var value, ok = s.structs.Load().(map[reflect.Type]structMetadata)[key]
	return value, ok
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

type structQueueElement struct {
	Type  reflect.Type
	Index []int
}

func (s *scanner) parseStruct(dstType reflect.Type) structMetadata {
	s.mu.Lock()

	var mStruct, ok = s.getStructMetadata(dstType)
	if ok {
		s.mu.Unlock()
		return mStruct
	}

	var queue = make([]structQueueElement, 0, 10)
	queue = append(queue, structQueueElement{
		Type:  dstType,
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

			if _, found := fields[tag]; found {
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

	s.setStructMetadata(dstType, mStruct)
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

func (s structMetadata) Field(columnType *sql.ColumnType) *fieldMetadata {
	var columnName = columnType.Name()
	var field = s.fields[columnName]
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
