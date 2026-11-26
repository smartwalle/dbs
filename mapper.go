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
	kTagSQL            = "sql"
	kTagValueDisable   = "-"
	kTagValueDefault   = "default"
	kTagValueSeparator = ";"
)

var ErrInvalidEncodeValue = errors.New("dbs: encode value must be a non-nil struct or struct pointer")

// Mapper 负责结构体和查询结果之间的转换。
type Mapper interface {
	// Encode 把结构体转成字段列表。
	Encode(src any) (values []Field, err error)

	// Decode 把查询结果解码到 dest 里，返回解码的行数。
	Decode(rows *sql.Rows, dest any) (int, error)
}

type mapper struct {
	tag     string
	structs atomic.Value // map[reflect.Type]structMetadata
	mu      sync.Mutex

	scannerType  reflect.Type
	rawBytesType reflect.Type

	values sync.Map // 缓存字段类型对应的扫描目标池
	zeros  sync.Map // 缓存字段类型对应的零值
}

// NewMapper 创建一个 Mapper，tag 是结构体字段上用来指定列名的标签名。
func NewMapper(tag string) Mapper {
	var m = &mapper{
		tag:          tag,
		scannerType:  reflect.TypeOf((*sql.Scanner)(nil)).Elem(),
		rawBytesType: reflect.TypeOf(sql.RawBytes{}),
	}
	m.structs.Store(make(map[reflect.Type]structMetadata))
	return m
}

// Encode 把结构体转成字段列表。
//
// src 必须是非空的结构体或者结构体指针。
// 带 default 标签的字段，值是零值时会转成 SQL("DEFAULT")。
func (m *mapper) Encode(src any) (fields []Field, err error) {
	// 剥掉指针，拿到真正的结构体类型和值。
	var srcType, srcValue, nErr = encodeBase(src)
	if nErr != nil {
		return nil, nErr
	}

	// 字段元数据按类型缓存，第二次开始就不用再遍历结构体。
	var mStruct, ok = m.getStructMetadata(srcType)
	if !ok {
		mStruct = m.buildStructMetadata(srcType)
	}

	// 按列名顺序输出，保证同样的结构体每次生成的字段顺序一样。
	fields = make([]Field, 0, len(mStruct.fields))
	for _, column := range mStruct.columns {
		var field = mStruct.fields[column]
		if field == nil {
			continue
		}

		// 按字段路径找值，路径中间的指针结构体是 nil 就跳过这个字段。
		var value, found = findFieldByIndex(srcValue, field.Index)
		if !found {
			continue
		}

		// 带 default 标签的字段，值是零值时用 DEFAULT 交给数据库填。
		var useDefault = false
		var useValue = value.Interface()
		if field.UseDefault && value.IsZero() {
			useDefault = true
			useValue = SQL("DEFAULT")
		}
		fields = append(fields, Field{Name: column, UseDefault: useDefault, Value: useValue})
	}
	return fields, nil
}

// encodeBase 剥掉 src 前面的指针，返回结构体类型和值。
func encodeBase(src any) (reflect.Type, reflect.Value, error) {
	if src == nil {
		return nil, reflect.Value{}, ErrInvalidEncodeValue
	}

	var srcValue = reflect.ValueOf(src)
	var srcType = srcValue.Type()

	// 一层层剥掉指针，剥到结构体为止。
	for srcType.Kind() == reflect.Pointer {
		if srcValue.IsNil() {
			return nil, reflect.Value{}, ErrInvalidEncodeValue
		}
		srcValue = srcValue.Elem()
		srcType = srcType.Elem()
	}

	// 不是结构体就没法按列取值。
	if srcType.Kind() != reflect.Struct {
		return nil, reflect.Value{}, ErrInvalidEncodeValue
	}

	return srcType, srcValue, nil
}

// Decode 把查询结果解码到 dest 里，返回解码的行数。
//
// dest 必须是指针，指向下面任意一种目标：
//   - 结构体或结构体指针：解一行；
//   - 结构体切片或结构体指针切片：解所有行；
//   - 基础类型切片：解一列；
//   - map 或 map 切片：解成「列名 → 值」。
func (m *mapper) Decode(rows *sql.Rows, dest any) (rowsAffected int, err error) {
	if rows == nil {
		return rowsAffected, sql.ErrNoRows
	}

	if dest == nil {
		return rowsAffected, errors.New("dbs: nil pointer passed")
	}

	var destValue = reflect.ValueOf(dest)
	var destType = destValue.Type()

	// dest 必须是指针，否则没法往里写值。
	if destValue.Kind() != reflect.Pointer {
		return rowsAffected, errors.New("dbs: must pass a pointer")
	}

	if destValue.IsNil() {
		return rowsAffected, errors.New("dbs: nil pointer passed")
	}

	// 上一条查询留下的错误要先报出来。
	if err = rows.Err(); err != nil {
		return rowsAffected, err
	}

	// 先取第一行，后面的循环从第二行开始。
	// 一行都没有时返回 sql.ErrNoRows，调用方常用它来判断「没查到」。
	if !rows.Next() {
		if err = rows.Err(); err != nil {
			return rowsAffected, err
		}
		return rowsAffected, sql.ErrNoRows
	}

	// 剥掉 dest 前面的指针，拿到真正的目标类型和值。
	destType, destValue = base(destType, destValue)

	if destType.Kind() == reflect.Slice {
		// 切片：解所有行，解出来的行追加在已有内容后面。
		// 行数用切片长度的前后差值算。
		var oldLen = destValue.Len()
		if err = m.scanSlice(rows, dest, destType, destValue); err != nil {
			return 0, err
		}
		rowsAffected = destValue.Len() - oldLen
	} else {
		// 单个值：只解第一行。
		if err = m.scanOne(rows, dest, destType, destValue); err != nil {
			return 0, err
		}
		rowsAffected = 1
	}
	return rowsAffected, nil
}

// prepare 准备结构体解码需要的字段信息和扫描目标，返回值见 scanTargets。
//
// 扫描目标统一是「指向字段类型的指针的指针」（**T），它有两层含义：
//   - 内层指针（*T）是快路径目标，database/sql 可以直接写进去，不用为每一行分配对象；
//   - 外层指针是兼容目标，NULL 落在非指针字段上时用它重扫，NULL 会被转成空指针。
//
// 两套目标读值的方式一致，都是取内层指针的 Elem，所以扫描和取值都不需要区分路径。
//
// 实现了 sql.Scanner 的字段不能走快路径：快路径会复用同一个实例，
// 字段内部的状态会在多行之间串行，这类字段改用兼容目标逐行新建实例。
func (m *mapper) prepare(destType reflect.Type, columns []string) scanTargets {
	// 字段元数据按类型缓存，这里第一次遇到这个类型就构建一次。
	var mStruct, ok = m.getStructMetadata(destType)
	if !ok {
		mStruct = m.buildStructMetadata(destType)
	}

	// 几组数据都按列的下标一一对应。
	var targets = scanTargets{
		fields: make([]*fieldMetadata, len(columns)),
		compat: make([]any, len(columns)),
		fast:   make([]any, len(columns)),
	}

	for idx, column := range columns {
		// 找这一列对应的结构体字段。
		var field = mStruct.Field(column)
		if field != nil {
			targets.fields[idx] = field

			// 兼容目标从对象池里拿，内层指针已经分配好了。
			// 池是按字段类型分的，所以同一类型的字段共用一个池。
			var compat = field.ValuePool.Get()
			targets.compat[idx] = compat

			if field.Scanner {
				// 实现了 sql.Scanner 的字段用包装器做扫描目标：
				// 包装器逐行新建实例，实例只属于这一行，
				// 兼容路径重扫同一行时也不会重复调用底层 Scanner。
				var wrapper = newScannerTarget(compat, field.Type)
				targets.scanners = append(targets.scanners, scannerEntry{
					index:  idx,
					target: wrapper,
				})
				// 快路径和兼容路径用同一个包装器，
				// 重扫时包装器会记得这一行已经调用过，不再重复调用。
				targets.fast[idx] = wrapper
				targets.compat[idx] = wrapper
			} else {
				// 普通字段用内层指针，也就是字段类型本身，
				// database/sql 直接往里写，不用为每一行分配对象。
				targets.fast[idx] = reflect.ValueOf(compat).Elem().Interface()
			}
		} else {
			// 没有对应结构体字段的列也要接收驱动返回的值，
			// 否则 Scan 的参数个数和列数对不上会直接报错。
			// 这里用 any 接收，取值时不会用到。
			var val any
			targets.compat[idx] = &val
			targets.fast[idx] = &val
		}
	}
	return targets
}

// innerTargets 取出一批扫描目标的内层指针。
//
// 内层指针指向的是目标对象里那个指针变量，地址一直不变，
// 兼容扫描把内层指针换掉时它照样指向新的那个，
// 所以可以提前取好，取值时直接用它的 Elem。
func innerTargets(targets scanTargets) []reflect.Value {
	var inner = make([]reflect.Value, len(targets.compat))
	for idx, field := range targets.fields {
		// Scanner 字段的扫描目标是包装器，取值不走这里。
		if field != nil && field.Scanner {
			continue
		}
		inner[idx] = reflect.ValueOf(targets.compat[idx]).Elem()
	}
	return inner
}

// releaseTargets 把扫描目标还回字段的池子里。
//
// 池子里的对象会被后面的解码直接拿去用，所以池子里的对象不能交到调用方手里，
// 取值时必须拷贝。
//
// 还回池子之前要把内层指针补上。
// 兼容扫描遇到 NULL 时会把内层指针置空，如果这时又出错退出，
// refreshFast 就来不及执行，空指针会被还回池子。
// 下一次解码拿到这个空指针当快路径目标，
// database/sql 往空指针里写值（比如 *time.Time）会直接 panic，
// 而且 panic 发生在 Scan 内部，Rows 的锁不会释放，后面的 Close 会卡死。
func (m *mapper) releaseTargets(targets scanTargets) {
	for idx, field := range targets.fields {
		// 没有对应字段的列，用的不是对象池里的目标，不用还。
		if field == nil {
			continue
		}

		// Scanner 字段的扫描目标是包装器，
		// 真正要还回池子的是它里面的目标。
		var holder = targets.compat[idx]
		if wrapper, ok := holder.(*scannerTarget); ok {
			holder = wrapper.holder
		}

		// holder 是「指向字段类型的指针的指针」，
		// 取一次 Elem 拿到内层指针，也就是扫描目标本身。
		var target = reflect.ValueOf(holder).Elem()

		// 内层指针是空的话要补一个。
		// 兼容扫描遇到 NULL 会把内层指针置空，
		// 如果这时又出错退出，refreshFast 就来不及执行了。
		if target.IsNil() {
			target.Set(reflect.New(field.Type))
		}

		// 还回池子。下一个解码会直接拿到这个对象继续用，
		// 所以池里的对象不能交到调用方手里，取值时必须拷贝。
		field.ValuePool.Put(holder)
	}
}

// scanOne 把查询结果解到一个结构体里，只取一行。
func (m *mapper) scanOne(rows *sql.Rows, dest any, destType reflect.Type, destValue reflect.Value) error {
	// 单值目标：结构体、基础类型、map 三种。
	switch destType.Kind() {
	case reflect.Struct:
		// 结构体只需要列名，ColumnTypes 会额外分配列信息。
		var columns, err = rows.Columns()
		if err != nil {
			return err
		}
		// 准备字段信息和扫描目标，解完还要把扫描目标还回池子。
		var targets = m.prepare(destType, columns)
		defer m.releaseTargets(targets)

		return m.scanIntoStruct(rows, columns, targets, destValue)
	case reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64,
		reflect.String:
		// 基础类型：一列一个值，扫进来直接写进目标。
		var nValue, err = scanScalar(rows, destType)
		if err != nil {
			return err
		}
		// 取不到值（比如列是 NULL）时保持目标原来的值，不写零值。
		if nValue.IsValid() {
			destValue.Set(nValue)
		}
	case reflect.Map:
		// map[string]any 要按列声明的类型接收驱动返回的值，所以这里需要列类型。
		var columns, err = rows.ColumnTypes()
		if err != nil {
			return err
		}
		// 只支持 key 是字符串的 map。
		if destType.Key().Kind() == reflect.String {
			switch destType.Elem().Kind() {
			case reflect.Bool:
				return scanIntoMap[bool](rows, columns, dest)
			case reflect.Int:
				return scanIntoMap[int](rows, columns, dest)
			case reflect.Int8:
				return scanIntoMap[int8](rows, columns, dest)
			case reflect.Int16:
				return scanIntoMap[int16](rows, columns, dest)
			case reflect.Int32:
				return scanIntoMap[int32](rows, columns, dest)
			case reflect.Int64:
				return scanIntoMap[int64](rows, columns, dest)
			case reflect.Uint:
				return scanIntoMap[uint](rows, columns, dest)
			case reflect.Uint8:
				return scanIntoMap[uint8](rows, columns, dest)
			case reflect.Uint16:
				return scanIntoMap[uint16](rows, columns, dest)
			case reflect.Uint32:
				return scanIntoMap[uint32](rows, columns, dest)
			case reflect.Uint64:
				return scanIntoMap[uint64](rows, columns, dest)
			case reflect.Float32:
				return scanIntoMap[float32](rows, columns, dest)
			case reflect.Float64:
				return scanIntoMap[float64](rows, columns, dest)
			case reflect.String:
				return scanIntoMap[string](rows, columns, dest)
			case reflect.Interface:
				// map[string]any 按列声明的类型接收，所以要列类型。
				return scanIntoMapAny(rows, columns, dest)
			default:
				// 其他元素类型不支持。
			}
		}
		return fmt.Errorf("map[%s]%s is unsupported", destType.Key().Kind(), destType.Elem().Kind())
	default:
		return fmt.Errorf("%s is unsupported", destType.Kind())
	}
	return nil
}

// scanSlice 把查询结果解到切片里。
//
// 切片可以是结构体切片、结构体指针切片、基础类型切片、map 切片。
// 切片里已有的内容不会被破坏，新解出来的行追加在后面。
func (m *mapper) scanSlice(rows *sql.Rows, dest any, destType reflect.Type, destValue reflect.Value) error {
	// 取切片的元素类型。
	destType = destType.Elem()

	// 元素是不是指针。指针元素每行是一个独立的对象。
	var isPointer = destType.Kind() == reflect.Pointer

	if isPointer {
		destType = destType.Elem()
	}

	switch destType.Kind() {
	case reflect.Struct:
		// 结构体只需要列名，ColumnTypes 会额外分配列信息。
		var columns, err = rows.Columns()
		if err != nil {
			return err
		}
		var targets = m.prepare(destType, columns)
		defer m.releaseTargets(targets)

		// 直接把每一行写进目标切片：不用为每一行单独分配元素对象，也不需要中间切片。
		// 出错时目标切片的长度保持不变，调用方已有的内容不会被破坏。
		//
		// list 是目标切片的可寻址副本：Grow 和 SetLen 需要可寻址的值，
		// 扫描过程中改的是副本，全部成功之后才写回 destValue，
		// 出错时目标切片的长度不变。
		//
		// 注意切片赋值只复制切片头，list 和目标切片共用同一个底层数组。
		// 所以出错时，目标切片长度之外的那段备用容量里可能留下半截数据，
		// 但目标切片的长度和里面的内容都不受影响，调用方看不到那些数据。
		var list = reflect.New(destValue.Type()).Elem()
		list.Set(destValue)

		// 零值每行都一样，先取好，循环里直接用。
		var zeroValue = m.getZeroValue(destType)

		// index 是下一个要写的下标，从已有内容的后面开始。
		var index = list.Len()
		// scanned 是已经扫过的行数，用来判断值不值得提前准备内层指针。
		var scanned int
		for {
			if index >= list.Len() {
				// 扩容交给内置 append：Grow 保证容量，SetLen 把长度加一。
				list.Grow(1)
				list.SetLen(index + 1)
			}
			// 当前这一行要写到的槽位。
			var slot = list.Index(index)
			index++

			// 扫描的目标：普通元素直接用槽位本身，
			// 指针元素新建一个对象放进去，槽位里存的是指针。
			var target = slot
			if isPointer {
				var pointer = reflect.New(destType)
				slot.Set(pointer)
				target = pointer.Elem()
			} else {
				// 复用容量时槽位里可能是上一轮的数据，先清零，
				// 这一轮没有查到的字段就是零值。
				slot.Set(zeroValue)
			}

			// 把当前行扫进目标对象。
			if err = m.scanIntoStruct(rows, columns, targets, target); err != nil {
				return err
			}
			scanned++

			// 没有下一行了就结束。
			if !rows.Next() {
				break
			}

			// 后面还有行，提前把内层指针取好，后面的行就不用每一行重新取了。
			// 只扫两三行的话提前取不划算，所以扫到第三行才准备。
			if scanned >= 2 && targets.inner == nil {
				targets.inner = innerTargets(targets)
			}
		}
		if err = rows.Err(); err != nil {
			return err
		}

		// 长度正好是写进去的行数，写回目标切片。
		destValue.Set(list)
	case reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64,
		reflect.String:
		// 扫描目标在多行之间复用，扫描后立刻把值取走。
		// 目标是类型本身的指针，database/sql 直接往里写。
		var target = reflect.New(destType)
		// 目标转成 any 的结果每行都一样，先转好，循环里直接用。
		var targetAny = target.Interface()
		// 兼容目标，用双层指针，NULL 落在非指针类型上时用它重扫。
		var compat reflect.Value
		// 兼容目标转成 any 的结果，兼容路径第一次用到时才准备。
		var compatAny any
		// 零值每行都一样，先取好，循环里直接用。
		var zeroValue = m.getZeroValue(destType)
		// 指针元素的目标。
		var pointer reflect.Value
		// 指针元素的目标转成 any 的结果，每行都一样，先转好。
		var pointerAny any
		if isPointer {
			// 指针元素：database/sql 会为每一行分配新的实例，
			// SQL NULL 对应 nil 指针，也需要保留当前行。
			pointer = reflect.New(reflect.PointerTo(destType))
			pointerAny = pointer.Interface()
		}
		// list 是目标切片的可寻址副本，理由和结构体分支一样。
		var list = reflect.New(destValue.Type()).Elem()
		list.Set(destValue)

		var index = list.Len()
		for {
			if index >= list.Len() {
				// 扩容交给内置 append：Grow 保证容量，SetLen 把长度加一。
				list.Grow(1)
				list.SetLen(index + 1)
			}
			var slot = list.Index(index)
			index++

			if isPointer {
				// 指针元素：扫进双层指针，database/sql 会给这一行新建一个实例，
				// NULL 时是 nil 指针。
				if err := rows.Scan(pointerAny); err != nil {
					return err
				}
				// 把实例存进槽位，取走的是指针，实例由这一行独占。
				slot.Set(pointer.Elem())
			} else {
				var nValue reflect.Value
				if err := rows.Scan(targetAny); err == nil {
					// 快路径：值直接写进了 target。
					nValue = target.Elem()
				} else {
					// 兼容路径：NULL 落在非指针类型上时 database/sql 会报错，
					// 改用双层指针重扫同一行，NULL 还原成零值。
					if !compat.IsValid() {
						compat = reflect.New(reflect.PointerTo(destType))
						compatAny = compat.Interface()
					}
					if err = rows.Scan(compatAny); err != nil {
						return err
					}
					// 取两次 Elem：第一次拿到指针，第二次拿到值。
					nValue = compat.Elem().Elem()
				}
				if nValue.IsValid() {
					// 值是拷贝，槽位和扫描目标不共享内存。
					slot.Set(nValue)
				} else {
					// 非指针元素遇到 SQL NULL 时使用零值，不能丢弃当前行。
					slot.Set(zeroValue)
				}
			}

			if !rows.Next() {
				break
			}
		}
		if err := rows.Err(); err != nil {
			return err
		}
		destValue.Set(list)
	case reflect.Map:
		// map[string]any 要按列声明的类型接收驱动返回的值，所以这里需要列类型。
		var columns, err = rows.ColumnTypes()
		if err != nil {
			return err
		}
		if destType.Key().Kind() == reflect.String {
			switch destType.Elem().Kind() {
			case reflect.Bool:
				return scanIntoMaps[bool](rows, columns, dest)
			case reflect.Int:
				return scanIntoMaps[int](rows, columns, dest)
			case reflect.Int8:
				return scanIntoMaps[int8](rows, columns, dest)
			case reflect.Int16:
				return scanIntoMaps[int16](rows, columns, dest)
			case reflect.Int32:
				return scanIntoMaps[int32](rows, columns, dest)
			case reflect.Int64:
				return scanIntoMaps[int64](rows, columns, dest)
			case reflect.Uint:
				return scanIntoMaps[uint](rows, columns, dest)
			case reflect.Uint8:
				return scanIntoMaps[uint8](rows, columns, dest)
			case reflect.Uint16:
				return scanIntoMaps[uint16](rows, columns, dest)
			case reflect.Uint32:
				return scanIntoMaps[uint32](rows, columns, dest)
			case reflect.Uint64:
				return scanIntoMaps[uint64](rows, columns, dest)
			case reflect.Float32:
				return scanIntoMaps[float32](rows, columns, dest)
			case reflect.Float64:
				return scanIntoMaps[float64](rows, columns, dest)
			case reflect.String:
				return scanIntoMaps[string](rows, columns, dest)
			case reflect.Interface:
				return scanIntoMapsAny(rows, columns, dest)
			default:
			}
		}
		return fmt.Errorf("[]map[%s]%s is unsupported", destType.Key().Kind(), destType.Elem().Kind())
	default:
		return fmt.Errorf("%s is unsupported", destType.Kind())
	}
	return nil
}

// scannerTarget 是 sql.Scanner 字段的扫描目标。
//
// 它交给 database/sql 的 Scan，database/sql 把列值交给它，由它转交给底层 Scanner。
// 它负责三件事：
//   - 每一行新建一个 Scanner 实例，实例只属于这一行，
//     字段内部的状态不会在行之间串；
//   - 兼容路径重扫同一行时不再重复调用底层 Scanner，
//     只把上一次的错误返回回去，避免副作用被执行两次；
//   - 把这一行的值取出来交给上层，复制进结构体。
type scannerTarget struct {
	// holder 是「指向字段类型的指针的指针」，实例就装在它的内层指针里。
	holder any

	// slot 是装着实例的那个槽，类型是「指向 Scanner 的指针」。
	slot reflect.Value

	// value 是这一行的值，列是 NULL 时是无效值。
	value reflect.Value

	// pointerField 表示字段本身是指针类型，值取实例的指针。
	pointerField bool

	// called 表示这一行已经调用过底层 Scanner。
	called bool

	// err 是底层 Scanner 返回的错误。
	err error
}

// newScannerTarget 包装一个扫描目标，给 sql.Scanner 字段用。
//
// holder 是「指向字段类型的指针的指针」，
// 实例装在它的内层指针里，每一行都会换成新的。
func newScannerTarget(holder any, fieldType reflect.Type) *scannerTarget {
	var t = &scannerTarget{
		holder:       holder,
		pointerField: fieldType.Kind() == reflect.Pointer,
	}

	// holder 取一次 Elem 是「指向字段类型的指针」，
	// 字段是指针类型时，实例在它的下一层。
	var slot = reflect.ValueOf(holder).Elem()
	if t.pointerField {
		slot = slot.Elem()
	}
	t.slot = slot
	return t
}

// Scan 实现 sql.Scanner，database/sql 会把列值交给它。
func (t *scannerTarget) Scan(src any) error {
	// 兼容路径重扫同一行：直接返回上一次的结果，
	// 不再重复调用底层 Scanner，副作用只会有一次。
	if t.called {
		return t.err
	}
	t.called = true
	t.err = nil
	t.value = reflect.Value{}

	// 列是 NULL：字段取零值，不调用底层 Scanner。
	if src == nil {
		return nil
	}

	// 每一行新建一个实例，实例只属于这一行。
	var instance = reflect.New(t.slot.Type().Elem())
	t.slot.Set(instance)

	// 调用底层 Scanner。
	var scanner, ok = instance.Interface().(sql.Scanner)
	if !ok {
		t.err = fmt.Errorf("dbs: %s does not implement sql.Scanner", t.slot.Type().Elem())
		return t.err
	}
	t.err = scanner.Scan(src)

	// 值类型字段取实例的内容，指针类型字段取实例的指针。
	if t.pointerField {
		t.value = t.slot
	} else {
		t.value = t.slot.Elem()
	}
	return t.err
}

// reset 清掉上一行的状态，下一行重新开始。
func (t *scannerTarget) reset() {
	t.called = false
	t.err = nil
	t.value = reflect.Value{}
}

// scannerEntry 记着 Scanner 字段的列下标和它的扫描目标。
type scannerEntry struct {
	// index 是列的下标。
	index int

	// target 是这一列的扫描目标。
	target *scannerTarget
}

// scanIntoStruct 把当前行扫描进结构体。
//
// 先用快路径目标扫描，失败基本只有一种原因：NULL 落在了非指针字段上，
// 此时 database/sql 会报错，改用兼容目标重扫同一行，NULL 会被转成空指针，
// 字段取零值，真正的类型错误则原样返回。
//
// 取值时都是把扫描目标里的值拷贝进结构体，调用方拿到的是拷贝，
// 不会和扫描目标共享内存，后面再解一行也不会改写已经解出来的内容。
func (m *mapper) scanIntoStruct(rows *sql.Rows, columns []string, targets scanTargets, destValue reflect.Value) error {
	// 只有一列、又没有对应的结构体字段时，这一列的值直接给目标对象。
	if len(columns) == 1 && targets.fields[0] == nil {
		switch destValue.Addr().Interface().(type) {
		case *time.Time:
			// 时间要用双层指针，NULL 时是 nil。
			var val *time.Time
			return scanSingleColumn(rows, columns[0], &val, destValue)
		case sql.Scanner:
			// 目标自己实现了 Scanner，交给它处理。
			return rows.Scan(destValue.Addr().Interface())
		}
		// 其他类型用 any 接住，再转成目标类型。
		var val any
		return scanSingleColumn(rows, columns[0], &val, destValue)
	}

	// 每一行都重新开始，Scanner 字段的包装器要清掉上一行的状态。
	for _, entry := range targets.scanners {
		entry.target.reset()
	}

	// useCompat 表示这一行走的是兼容路径。
	var useCompat = false

	// 快路径：用内层指针扫描，每一行都不需要分配对象。
	if err := rows.Scan(targets.fast...); err != nil {
		// 兼容路径：NULL 落在非指针字段上时 database/sql 会报错，
		// 改用兼容目标重扫同一行，NULL 会被转成空指针，字段取零值。
		// 兼容目标多一层指针，database/sql 会为这一行新建一个实例。
		if err = rows.Scan(targets.compat...); err != nil {
			// 兼容路径也失败，说明是真的类型错误，原样返回。
			return err
		}
		useCompat = true
	}

	// Scanner 字段的值由包装器保管，单独写进结构体。
	for _, entry := range targets.scanners {
		var field = targets.fields[entry.index]
		var value = entry.target.value
		if value.IsValid() {
			// 写进去的是值的拷贝，结构体和扫描目标不共享内存。
			// 后面再解一行，也不会改写已经写好的内容。
			fieldByIndex(destValue, field.Index).Set(value)
		} else {
			// 列是 NULL，字段取零值。
			fieldByIndex(destValue, field.Index).Set(reflect.Zero(field.Type))
		}
	}

	// 把扫描目标里的值写进结构体。
	for idx, field := range targets.fields {
		// 没有对应字段的列不用管。
		if field == nil {
			continue
		}
		// Scanner 字段的值上面已经写进去了。
		if field.Scanner {
			continue
		}

		// 取内层指针的 Elem 才是字段的值。
		// 快路径和兼容路径都是这个结构，读值的写法一样。
		// 多行的时候 inner 已经提前取好了，直接用；
		// 单行的时候 inner 是空的，按原来的方式取。
		var value reflect.Value
		if targets.inner != nil {
			value = targets.inner[idx].Elem()
		} else {
			value = reflect.ValueOf(targets.compat[idx]).Elem().Elem()
		}

		if value.IsValid() {
			if field.RawBytes {
				// RawBytes 的数据来自驱动缓冲区，下一次读行就会被覆盖，
				// 必须复制一份再写进结构体。
				fieldByIndex(destValue, field.Index).Set(m.cloneRawBytes(value))
			} else {
				// 写进去的是值的拷贝，结构体和扫描目标不共享内存。
				// 后面再解一行，也不会改写这里已经写好的内容。
				fieldByIndex(destValue, field.Index).Set(value)
			}
		} else {
			// 兼容路径上遇到 NULL，字段取零值。
			fieldByIndex(destValue, field.Index).Set(reflect.Zero(field.Type))
		}
	}

	// 走了兼容路径就把快路径目标刷新一下，下一行才能继续走快路径。
	if useCompat {
		m.refreshFast(targets)
	}
	return nil
}

// refreshFast 在兼容扫描之后把快路径目标刷新一遍。
//
// 兼容扫描会换掉扫描目标里的内层指针，快路径目标还指着旧对象；
// NULL 造成的空指针也要补上，下一行才能继续走快路径。
// 没有这一步，下一行会扫进空指针，database/sql 往空指针里写值会 panic。
func (m *mapper) refreshFast(targets scanTargets) {
	for idx, field := range targets.fields {
		if field == nil {
			continue
		}
		// Scanner 字段的实例由包装器自己换，这里不用管。
		if field.Scanner {
			continue
		}

		// 兼容扫描把内层指针换掉了。
		// inner 指向的还是那个指针变量，拿到的就是换过之后的新指针；
		// 没有 inner 时现取。
		var target reflect.Value
		if targets.inner != nil {
			target = targets.inner[idx]
		} else {
			target = reflect.ValueOf(targets.compat[idx]).Elem()
		}

		// NULL 会把内层指针置空，补一个回去，
		// 不然下一行扫进空指针会 panic。
		if target.IsNil() {
			target.Set(reflect.New(field.Type))
		}

		// Scanner 字段的快路径目标一直是外层指针，不用改。
		if !field.Scanner {
			// 快路径目标换成新的内层指针。
			targets.fast[idx] = target.Interface()
		}
	}
}

// scanSingleColumn 把唯一一列的值扫描进目标对象。
//
// 目标对象可以是时间、实现了 sql.Scanner 的类型，或者普通结构体，
// 取不到值时填零值。
func scanSingleColumn(rows *sql.Rows, name string, target any, destValue reflect.Value) error {
	if err := rows.Scan(target); err != nil {
		return err
	}

	// 取两次 Elem 拿到值。
	var value = reflect.ValueOf(target).Elem().Elem()
	if !value.IsValid() {
		// 列是 NULL，目标填零值。
		destValue.Set(reflect.Zero(destValue.Type()))
	} else if value.Type().AssignableTo(destValue.Type()) {
		// 类型正好一样，直接写。
		destValue.Set(value)
	} else if value.Type().ConvertibleTo(destValue.Type()) {
		// 类型可以转换，转换之后再写。
		destValue.Set(value.Convert(destValue.Type()))
	} else {
		// 类型对不上，报错。
		return fmt.Errorf(
			"dbs: cannot assign column %q value of type %s to %s",
			name,
			value.Type(),
			destValue.Type(),
		)
	}
	return nil
}

// scanScalar 把一列数据扫描进基础类型。
//
// 快路径用类型本身的指针作目标，database/sql 可以直接写进去，不用分配中间对象；
// NULL 在非指针目标上会报错，这时改用双层指针重扫，NULL 还原成零值。
func scanScalar(rows *sql.Rows, destType reflect.Type) (reflect.Value, error) {
	// 快路径：目标是类型本身的指针，database/sql 直接写进去。
	var target = reflect.New(destType)
	if err := rows.Scan(target.Interface()); err == nil {
		return target.Elem(), nil
	}

	// 兼容路径：NULL 落在非指针目标上会报错，
	// 改用双层指针重扫同一行，NULL 还原成零值。
	var compat = reflect.New(reflect.PointerTo(destType))
	if err := rows.Scan(compat.Interface()); err != nil {
		return reflect.Value{}, err
	}
	return compat.Elem().Elem(), nil
}

// scanIntoMap 把查询结果解到一个 map 里。
//
// map 的元素类型是基础类型时，按元素类型接收驱动返回的值，
// map 里的值类型和元素类型一致。
//
// 这个文件和下面几个 map 函数都是泛型函数，
// Go 不允许定义泛型方法，所以只能放在包级。
func scanIntoMap[T any](rows *sql.Rows, columns []*sql.ColumnType, dest any) error {
	// dest 必须是 *map[string]T。
	var mapValue, ok = dest.(*map[string]T)
	if !ok {
		return fmt.Errorf("%+v is unsupported", dest)
	}
	// 调用方传进来的是 nil map，这里建一个。
	// 列数是已知的，预先给足容量，省掉中途扩容。
	if *mapValue == nil {
		*mapValue = make(map[string]T, len(columns))
	}
	// 扫描目标在多行之间复用，这里只有一行。
	var targets = mapValueTargets[T](columns)
	return scanIntoMapValue[T](rows, targets, *mapValue)
}

// scanIntoMaps 把查询结果解到 map 切片里，一行一个 map。
//
// 每一行都新建一个 map，行与行之间不共享内存。
func scanIntoMaps[T any](rows *sql.Rows, columns []*sql.ColumnType, dest any) error {
	// dest 必须是 *[]map[string]T。
	var mapValues, ok = dest.(*[]map[string]T)
	if !ok {
		return fmt.Errorf("%+v is unsupported", dest)
	}

	if *mapValues == nil {
		*mapValues = []map[string]T{}
	}

	// 扫描目标在多行之间复用，每一行扫完就把值取走。
	var targets = mapValueTargets[T](columns)
	for {
		// 每一行都新建一个 map，行与行之间不共享内存。
		var mapValue = make(map[string]T, len(columns))
		if err := scanIntoMapValue[T](rows, targets, mapValue); err != nil {
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

// mapTargets 准备基础类型 map 的扫描目标，多行之间可以复用。
//
// 目标是「指向元素类型的指针」，database/sql 直接把驱动返回的值
// 转成元素类型写进去，不用为每一行分配对象。
type mapTargets[T any] struct {
	// fast 是扫描目标。
	fast []any

	// names 是列名，扫描之前先取好，扫描的时候直接用。
	names []string
}

// mapValueTargets 准备基础类型 map 的扫描目标。
func mapValueTargets[T any](columns []*sql.ColumnType) *mapTargets[T] {
	var targets = &mapTargets[T]{
		fast:  make([]any, len(columns)),
		names: make([]string, len(columns)),
	}
	for idx, column := range columns {
		targets.names[idx] = column.Name()
		// 每一列一个「指向元素类型的指针」。
		var val T
		targets.fast[idx] = &val
	}
	return targets
}

// scanIntoMapValue 把当前行扫描进一个 map。
//
// 扫描之后把值取出来写进 map，写进去的是值的副本，
// 不会和扫描目标共享内存。
func scanIntoMapValue[T any](rows *sql.Rows, targets *mapTargets[T], mapValue map[string]T) error {
	// 把这一行扫进扫描目标。
	if err := rows.Scan(targets.fast...); err != nil {
		return err
	}

	for idx, name := range targets.names {
		// 扫描目标是「指向元素类型的指针」，断言之后直接取到值：
		// 不用反射，也不用把值装箱成接口，省掉每行每列的一次分配。
		pointer, ok := targets.fast[idx].(*T)
		if !ok {
			return fmt.Errorf("dbs: cannot assign column %q value to map value", name)
		}
		// 写进 map 的是值的副本，不和扫描目标共享内存。
		mapValue[name] = *pointer
	}
	return nil
}

// scanIntoMapAny 把查询结果解到一个 map[string]any 里。
//
// 值按列声明的类型接收，所以 map 里的值类型和列类型一致。
func scanIntoMapAny(rows *sql.Rows, columns []*sql.ColumnType, dest any) error {
	// dest 必须是 *map[string]any。
	var mapValue, ok = dest.(*map[string]any)
	if !ok {
		return fmt.Errorf("%+v is unsupported", dest)
	}
	// 调用方传进来的是 nil map，这里建一个。
	// 列数是已知的，预先给足容量，省掉中途扩容。
	if *mapValue == nil {
		*mapValue = make(map[string]any, len(columns))
	}
	var targets = newMapAnyTargets(columns)
	return scanIntoMapAnyValue(rows, targets, *mapValue)
}

// scanIntoMapsAny 把查询结果解到 map 切片里，一行一个 map。
//
// 每一行都新建一个 map，行与行之间不共享内存。
func scanIntoMapsAny(rows *sql.Rows, columns []*sql.ColumnType, dest any) error {
	// dest 必须是 *[]map[string]any。
	var mapValues, ok = dest.(*[]map[string]any)
	if !ok {
		return fmt.Errorf("%+v is unsupported", dest)
	}

	if *mapValues == nil {
		*mapValues = []map[string]any{}
	}

	var targets = newMapAnyTargets(columns)
	for {
		// 每一行都新建一个 map，行与行之间不共享内存。
		var mapValue = make(map[string]any, len(columns))
		if err := scanIntoMapAnyValue(rows, targets, mapValue); err != nil {
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

// mapAnyTargets 按列声明的类型准备 map[string]any 的扫描目标，多行之间可以复用。
//
// 快路径目标是「指向列类型的指针」，database/sql 直接把值转成列类型写进去，
// 不用为每一行分配对象；NULL 落在非指针列类型上时快路径会报错。
//
// 兼容路径目标是「指向列类型的指针的指针」，NULL 会变成空指针，
// 快路径失败时用它重扫同一行，值取零值。
// 兼容目标只有真的遇到 NULL 才用得上，所以等第一次失败时再准备。
type mapAnyTargets struct {
	// fast 是快路径扫描目标。
	fast []any

	// compat 是兼容路径扫描目标，第一次遇到 NULL 时才准备。
	compat []any

	// names 是列名，扫描之前先取好，扫描的时候直接用。
	names []string

	// columns 是原始的列信息，准备兼容目标的时候还要用。
	columns []*sql.ColumnType
}

// newMapAnyTargets 按列声明的类型准备扫描目标。
func newMapAnyTargets(columns []*sql.ColumnType) *mapAnyTargets {
	var targets = &mapAnyTargets{
		fast:    make([]any, len(columns)),
		names:   make([]string, len(columns)),
		columns: columns,
	}

	for idx, column := range columns {
		targets.names[idx] = column.Name()

		var scanType = column.ScanType()
		if scanType != nil {
			// 快路径：指向列类型的指针。
			targets.fast[idx] = reflect.New(scanType).Interface()
		} else {
			// 驱动没有给列类型，按驱动返回的原始值接收。
			// 快路径目标是 any，NULL 会被写成 nil，不会报错。
			var val any
			targets.fast[idx] = &val
		}
	}
	return targets
}

// buildCompat 在第一次需要兼容路径时准备兼容目标。
//
// 兼容目标是「指向列类型的指针的指针」，用两层指针是为了让 NULL 变成空指针。
// 目标建好之后多行之间一直复用。
func (t *mapAnyTargets) buildCompat() {
	if t.compat != nil {
		return
	}
	// any 本身的类型，没有列类型的列用它建「any 的指针的指针」。
	var anyType = reflect.TypeOf((*any)(nil)).Elem()

	t.compat = make([]any, len(t.fast))
	for idx, column := range t.columns {
		var scanType = column.ScanType()
		if scanType != nil {
			t.compat[idx] = reflect.New(reflect.PointerTo(scanType)).Interface()
		} else {
			// 两层指针和快路径一样，取值时不会多剥一层。
			t.compat[idx] = reflect.New(reflect.PointerTo(anyType)).Interface()
		}
	}
}

// scanIntoMapAnyValue 把当前行扫描进一个 map。
//
// 扫描之后把值取出来写进 map，写进去的是值的副本，
// 不会和扫描目标共享内存。
func scanIntoMapAnyValue(rows *sql.Rows, targets *mapAnyTargets, mapValue map[string]any) error {
	// 快路径：目标是「指向列类型的指针」，不用为每一行分配对象。
	if err := rows.Scan(targets.fast...); err != nil {
		// 兼容路径：NULL 落在非指针列类型上时快路径会报错，
		// 用兼容目标重扫同一行，NULL 变成空指针，值取零值。
		targets.buildCompat()
		if err = rows.Scan(targets.compat...); err != nil {
			return err
		}
		return fillMapAny(targets, targets.compat, mapValue, true)
	}
	return fillMapAny(targets, targets.fast, mapValue, false)
}

// fillMapAny 把扫描目标里的值写进 map。
//
// compat 为 true 时目标多一层指针，取两次 Elem；
// 取不到值说明这一列是 NULL，map 里放零值。
//
// 驱动没有给列类型时，目标是 any，NULL 会写成 nil，
// 取出来的也是一个合法的 nil 值，和其他列的 NULL 一样放进 map。
func fillMapAny(targets *mapAnyTargets, values []any, mapValue map[string]any, compat bool) (err error) {
	for idx, name := range targets.names {
		// 快路径目标是「指向列类型的指针」，取一次 Elem 就是值；
		// 兼容路径目标多一层指针，取两次 Elem。
		var value reflect.Value
		if compat {
			value = reflect.Indirect(reflect.Indirect(reflect.ValueOf(values[idx])))
		} else {
			value = reflect.Indirect(reflect.ValueOf(values[idx]))
		}

		if !value.IsValid() {
			// 列是 NULL，map 里放零值。
			mapValue[name] = nil
			continue
		}

		// 取出值。实现了 driver.Valuer 的值先转成驱动能用的类型。
		var nValue = value.Interface()
		if valuer, ok := nValue.(driver.Valuer); ok {
			if nValue, err = valuer.Value(); err != nil {
				return err
			}
		} else if b, ok := nValue.(sql.RawBytes); ok {
			// RawBytes 的数据来自驱动缓冲区，转成字符串断开引用。
			nValue = string(b)
		}

		// 写进 map 的是值的副本，不和扫描目标共享内存。
		mapValue[name] = nValue
	}
	return nil
}

// base 剥掉目标前面的指针，返回真正的目标类型和值。
//
// 目标是指针时会先分配好，保证后面的字段可以赋值。
func base(destType reflect.Type, destValue reflect.Value) (reflect.Type, reflect.Value) {
	for {
		// 指针是 nil 就先分配一个，保证后面能写值。
		if destValue.Kind() == reflect.Pointer && destValue.IsNil() {
			destValue.Set(reflect.New(destType.Elem()))
		}

		// 剥掉这一层指针。
		if destValue.Kind() == reflect.Pointer {
			destValue = destValue.Elem()
			destType = destType.Elem()
			continue
		}
		break
	}
	return destType, destValue
}

// findFieldByIndex 按字段路径找到字段。
//
// 路径里带指针结构体时，中间那层是 nil 就返回 false，这个字段取不到值。
func findFieldByIndex(value reflect.Value, index []int) (reflect.Value, bool) {
	for i, x := range index {
		// 路径中间的指针结构体是 nil，说明取不到值。
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

// fieldByIndex 按字段路径找到字段，可以写值。
//
// 路径里带指针结构体时，中间那层是 nil 会先分配一个，保证字段可以写值。
func fieldByIndex(value reflect.Value, index []int) reflect.Value {
	// 常见情况：字段就在第一层。
	if len(index) == 1 {
		return value.Field(index[0])
	}
	for i, x := range index {
		if i > 0 {
			// 路径中间的指针结构体是 nil 就先分配一个，
			// 否则字段没地方写。
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

// element 是构建字段元数据时的待处理项。
//
// Type 是当前这一层的结构体类型，Index 是到这一层为止的字段路径。
type element struct {
	// Type 是要展开的结构体类型。
	Type reflect.Type

	// Index 是这个结构体在外层结构体里的字段路径。
	Index []int

	// Ancestors 是展开到当前类型经过的类型，按从外到内的顺序。
	//
	// 展开的时候用它判断是不是递归：
	// 路径上已经出现过的类型不再展开，
	// 不然像「父节点指向自己」这种结构会一直展开下去。
	// 只看当前路径，不看全局，
	// 所以同一个类型通过不同的兄弟字段出现时仍然会正常展开。
	Ancestors []reflect.Type
}

// appendAncestor 在展开路径上追加一个类型，返回新的路径。
//
// 复制一份再追加，兄弟分支之间才不会互相影响。
func appendAncestor(ancestors []reflect.Type, t reflect.Type) []reflect.Type {
	var nAncestors = make([]reflect.Type, len(ancestors)+1)
	copy(nAncestors, ancestors)
	nAncestors[len(ancestors)] = t
	return nAncestors
}

// hasAncestor 判断类型是不是已经在展开路径上出现过。
func hasAncestor(ancestors []reflect.Type, t reflect.Type) bool {
	for _, item := range ancestors {
		if item == t {
			return true
		}
	}
	return false
}

// appendFieldIndex 给字段路径追加一层，返回一个新的切片。
//
// 每次都新建切片，避免多个字段共享同一个底层数组。
func appendFieldIndex(index []int, fieldIndex int) []int {
	var result = make([]int, len(index)+1)
	copy(result, index)
	result[len(result)-1] = fieldIndex
	return result
}

// buildStructMetadata 解析结构体，得到列名和字段的对应关系。
//
// 字段带 tag 时，tag 的第一个值就是列名；
// 字段不带 tag 时，如果是结构体或者结构体指针，就继续往里展开，
// 用里面的字段的 tag 当列名；其他字段直接忽略。
//
// 展开的每一层都记在 Index 里，赋值时按这个路径找到字段；
// 路径里的指针结构体如果是 nil，赋值时会自动分配。
func (m *mapper) buildStructMetadata(destType reflect.Type) structMetadata {
	// 同一时间只允许一个 goroutine 构建元数据：
	// 构建过程要递归遍历结构体，并发构建会得到不一致的结果。
	m.mu.Lock()
	defer m.mu.Unlock()

	// 可能别的 goroutine 已经建好了。
	var mStruct, ok = m.getStructMetadata(destType)
	if ok {
		return mStruct
	}

	// 用队列做广度优先遍历，展开嵌套的结构体。
	var queue = make([]element, 0, 10)
	queue = append(queue, element{
		Type:  destType,
		Index: nil,
		// Ancestors 是展开到当前类型经过的类型，
		// 用来发现递归：路径上已经出现过的类型不再展开。
		Ancestors: nil,
	})

	// 列名到字段信息的对应关系。
	var fields = make(map[string]*fieldMetadata)
	// 结构体能解出来的列名，顺序就是字段的顺序。
	var columns = make([]string, 0)

	for len(queue) > 0 {
		var current = queue[0]
		queue = queue[1:]

		var numField = current.Type.NumField()

		for i := 0; i < numField; i++ {
			var fieldStruct = current.Type.Field(i)

			// 非导出字段不能赋值，跳过。
			if fieldStruct.PkgPath != "" {
				continue
			}

			// 取标签，标签里的第一个值是列名。
			var tag = fieldStruct.Tag.Get(m.tag)

			// 标签是 - 表示这个字段不要参与。
			if tag == kTagValueDisable {
				continue
			}

			// 没有标签：结构体和结构体指针继续往里展开，
			// 用里面的字段当列；其他字段没有列名，直接忽略。
			if tag == "" {
				if fieldStruct.Type.Kind() == reflect.Pointer && fieldStruct.Type.Elem().Kind() == reflect.Struct {
					// 递归结构体（比如父节点指向自己）在这里停下，
					// 不然会一直展开下去。
					if hasAncestor(current.Ancestors, fieldStruct.Type.Elem()) {
						continue
					}
					queue = append(queue, element{
						Type:      fieldStruct.Type.Elem(),
						Index:     appendFieldIndex(current.Index, i),
						Ancestors: appendAncestor(current.Ancestors, current.Type),
					})
					continue
				}

				if fieldStruct.Type.Kind() == reflect.Struct {
					if hasAncestor(current.Ancestors, fieldStruct.Type) {
						continue
					}
					queue = append(queue, element{
						Type:      fieldStruct.Type,
						Index:     appendFieldIndex(current.Index, i),
						Ancestors: appendAncestor(current.Ancestors, current.Type),
					})
					continue
				}
				continue
			}

			// 标签可以写多个值，用分号隔开，第一个是列名，其余是开关。
			var tagValues = strings.Split(tag, kTagValueSeparator)
			var fieldName = strings.TrimSpace(tagValues[0])
			if fieldName == "" {
				continue
			}
			// 同名列只认第一个。
			if _, ok = fields[fieldName]; ok {
				continue
			}

			// 把剩下的标签值转成开关集合，比如 default。
			var tagMap = make(map[string]bool)
			for _, tagValue := range tagValues {
				tagMap[strings.ToLower(strings.TrimSpace(tagValue))] = true
			}

			var field = &fieldMetadata{}
			// 字段路径，记着它在结构体里的位置。
			field.Index = appendFieldIndex(current.Index, i)
			field.Type = fieldStruct.Type
			// 扫描目标池按字段类型分，同类型的字段共用一个池。
			field.ValuePool = m.getValuePool(field.Type)
			// 实现了 sql.Scanner 的字段要逐行新建实例。
			field.Scanner = m.isScannerField(field.Type)
			// RawBytes 的数据来自驱动缓冲区，要复制。
			field.RawBytes = m.isRawBytesField(field.Type)
			field.UseDefault = tagMap[kTagValueDefault]
			fields[fieldName] = field
			columns = append(columns, fieldName)
		}
	}
	mStruct.columns = columns
	mStruct.fields = fields

	// 缓存起来，下次直接用。
	m.setStructMetadata(destType, mStruct)

	return mStruct
}

// scanTargets 是一次结构体解码用到的扫描目标。
//
// 几组数据都按列的下标一一对应。
type scanTargets struct {
	// fields 是每一列对应的字段信息，没有对应字段的列是 nil。
	fields []*fieldMetadata

	// compat 是兼容目标，每一列一个，指向「指向字段类型的指针的指针」。
	compat []any

	// fast 是快路径目标，每一列一个，指向字段类型本身。
	fast []any

	// scanners 是 sql.Scanner 字段的扫描目标，
	// 只有实现了 sql.Scanner 的字段才有，其他列不占位置。
	scanners []scannerEntry

	// inner 是兼容目标的内层指针，每一列一个。
	//
	// inner 指向的是目标对象里那个指针变量，地址不会变，
	// 兼容扫描把内层指针换掉时它照样指向新的那个，
	// 所以取值时直接取它的 Elem，不用每一行重新取。
	//
	// 只有多行的时候才需要它：单行取一次就够了，提前取反而是浪费，
	// 所以这里是空的，scanSlice 扫完第一行之后才把它填上。
	inner []reflect.Value
}

// fieldMetadata 是一个字段的解码信息。
type fieldMetadata struct {
	// Index 是字段在结构体里的路径，展开嵌套结构体时会有多层。
	Index []int

	// Type 是字段的类型。
	Type reflect.Type

	// ValuePool 是这个字段类型的扫描目标池。
	ValuePool *sync.Pool

	// Scanner 表示该字段实现了 sql.Scanner，需要逐行新建实例。
	Scanner bool

	// RawBytes 表示该字段的类型是 sql.RawBytes（或它的指针）。
	//
	// RawBytes 的数据来自驱动的缓冲区，database/sql 不会复制，
	// 下一次读行就会覆盖这块内存，写进结构体时要先复制一份。
	RawBytes bool

	// UseDefault 表示这个字段用了 default 标签。
	UseDefault bool
}

// structMetadata 是一个结构体的解码信息。
type structMetadata struct {
	// columns 是结构体里能解出来的列名。
	columns []string

	// fields 是列名到字段信息的对应关系。
	fields map[string]*fieldMetadata
}

// Field 按列名找字段信息。
func (s structMetadata) Field(name string) *fieldMetadata {
	var field = s.fields[name]
	return field
}

// isScannerField 判断字段是否需要逐行新建实例。
//
// 字段类型是指针时，database/sql 会为每一行分配新的实例；
// 字段类型不是指针、但它的指针实现了 sql.Scanner 时，复用同一个实例会让
// 字段内部的状态在多行之间串行，因此必须逐行新建。
// 这类字段的快路径目标改成兼容目标，让 database/sql 每行新建实例。
func (m *mapper) isScannerField(fieldType reflect.Type) bool {
	// 字段本身实现了 sql.Scanner，一般是指针类型。
	if fieldType.Implements(m.scannerType) {
		return true
	}
	// 字段是值类型，取地址之后实现了 sql.Scanner。
	return reflect.PointerTo(fieldType).Implements(m.scannerType)
}

// isRawBytesField 判断字段类型是不是 sql.RawBytes 或它的指针。
//
// 这两种字段的数据都直接来自驱动缓冲区，需要特殊处理。
func (m *mapper) isRawBytesField(fieldType reflect.Type) bool {
	if fieldType == m.rawBytesType {
		return true
	}
	return fieldType.Kind() == reflect.Pointer && fieldType.Elem() == m.rawBytesType
}

// cloneRawBytes 复制 RawBytes 的内容，返回一份不依赖驱动缓冲区的数据。
//
// RawBytes 的数据来自驱动的缓冲区，database/sql 直接把缓冲区指针塞进来，
// 下一次读行就会覆盖这块内存。解码结果直接引用它的话，
// 前面已经解出来的行会被后面的行改写，所以这里必须复制一份。
//
// 字段是 *sql.RawBytes 时，复制的是它指向的内容，并且返回一个新的指针，
// 这样结构体里拿到的指针也不会指向别人的内存。
func (m *mapper) cloneRawBytes(value reflect.Value) reflect.Value {
	// 字段类型是 *sql.RawBytes 时，复制的是它指向的内容，
	// 并且返回一个新的指针，结构体里拿到的指针不会指向别人的内存。
	if value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return value
		}
		var inner = value.Elem()
		var cloned = reflect.MakeSlice(m.rawBytesType, inner.Len(), inner.Len())
		reflect.Copy(cloned, inner)
		var pointer = reflect.New(m.rawBytesType)
		pointer.Elem().Set(cloned)
		return pointer
	}
	// 字段类型是 sql.RawBytes，复制内容。
	var cloned = reflect.MakeSlice(m.rawBytesType, value.Len(), value.Len())
	reflect.Copy(cloned, value)
	return cloned
}

// getValuePool 返回字段类型的扫描目标池。
//
// 池里的目标是「指向字段类型的指针的指针」，内层指针提前分配好，
// 快路径扫描时可以直接写进去，不用为每一行分配对象。
//
// 池是按类型全局共享的，同一个类型在不同的 Mapper 之间也共用一个池，
// 所以池里的对象不能交到调用方手里，取值时必须拷贝。
func (m *mapper) getValuePool(reflectType reflect.Type) *sync.Pool {
	// 池按类型缓存，同一个类型共用一个池。
	var pool, ok = m.values.Load(reflectType)
	if ok {
		return pool.(*sync.Pool)
	}
	var nPool = &sync.Pool{
		New: func() any {
			// 目标是「指向字段类型的指针的指针」，
			// 内层指针提前分配好，快路径扫描时可以直接写进去。
			var target = reflect.New(reflect.PointerTo(reflectType))
			target.Elem().Set(reflect.New(reflectType))
			return target.Interface()
		},
	}
	// 多个 goroutine 可能同时建池，用 LoadOrStore 保证只有一个生效。
	var actual, _ = m.values.LoadOrStore(reflectType, nPool)
	return actual.(*sync.Pool)
}

// getZeroValue 返回指定类型的零值。
//
// 零值是只读的，复制它不会产生额外分配，所以这里按类型缓存一份。
func (m *mapper) getZeroValue(reflectType reflect.Type) reflect.Value {
	// 零值按类型缓存，复制它不会产生额外分配。
	if value, ok := m.zeros.Load(reflectType); ok {
		return value.(reflect.Value)
	}
	var zero = reflect.Zero(reflectType)
	var actual, _ = m.zeros.LoadOrStore(reflectType, zero)
	return actual.(reflect.Value)
}

type Field struct {
	Name       string
	Value      any
	UseDefault bool
}
