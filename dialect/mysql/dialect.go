package mysql

import (
	"database/sql/driver"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"time"

	"github.com/smartwalle/dbs"
)

var _dialect = &dialect{}

const (
	kPlaceholder = '?'
)

func Dialect() dbs.Dialect {
	return _dialect
}

type dialect struct {
}

func (d *dialect) WritePlaceholder(w dbs.Writer, _ int) (err error) {
	if err = w.WriteByte(kPlaceholder); err != nil {
		return err
	}
	return nil
}

var convertibleTypes = []reflect.Type{
	reflect.TypeOf(time.Time{}),
}

func (d *dialect) WriteArgument(w dbs.Writer, arg any) error {
	if arg == nil {
		return writeString(w, "NULL")
	}

	var value = reflect.ValueOf(arg)
	if value.Kind() == reflect.Ptr && value.IsNil() {
		return writeString(w, "NULL")
	}

	switch raw := arg.(type) {
	case driver.Valuer:
		v, err := raw.Value()
		if err != nil {
			return err
		}
		return d.WriteArgument(w, v)
	case time.Time:
		return writeTime(w, raw)
	case bool:
		return writeString(w, strconv.FormatBool(raw))
	case string:
		return writeQuotedString(w, raw)
	case []byte:
		return writeBytes(w, raw)
	case int:
		return writeString(w, strconv.FormatInt(int64(raw), 10))
	case int8:
		return writeString(w, strconv.FormatInt(int64(raw), 10))
	case int16:
		return writeString(w, strconv.FormatInt(int64(raw), 10))
	case int32:
		return writeString(w, strconv.FormatInt(int64(raw), 10))
	case int64:
		return writeString(w, strconv.FormatInt(raw, 10))
	case uint:
		return writeString(w, strconv.FormatUint(uint64(raw), 10))
	case uint8:
		return writeString(w, strconv.FormatUint(uint64(raw), 10))
	case uint16:
		return writeString(w, strconv.FormatUint(uint64(raw), 10))
	case uint32:
		return writeString(w, strconv.FormatUint(uint64(raw), 10))
	case uint64:
		return writeString(w, strconv.FormatUint(raw, 10))
	case float32:
		return writeFloat(w, float64(raw), 32)
	case float64:
		return writeFloat(w, raw, 64)
	default:
		return d.writeReflectArgument(w, value, arg)
	}
}

func (d *dialect) writeReflectArgument(w dbs.Writer, value reflect.Value, arg any) error {
	switch value.Kind() {
	case reflect.Ptr:
		if value.IsNil() {
			return writeString(w, "NULL")
		}
		return d.WriteArgument(w, value.Elem().Interface())
	case reflect.Bool:
		return writeString(w, strconv.FormatBool(value.Bool()))
	case reflect.String:
		return writeQuotedString(w, value.String())
	case reflect.Int,
		reflect.Int8,
		reflect.Int16,
		reflect.Int32,
		reflect.Int64:
		return writeString(w, strconv.FormatInt(value.Int(), 10))
	case reflect.Uint,
		reflect.Uint8,
		reflect.Uint16,
		reflect.Uint32,
		reflect.Uint64:
		return writeString(w, strconv.FormatUint(value.Uint(), 10))
	case reflect.Float32:
		return writeFloat(w, value.Float(), 32)
	case reflect.Float64:
		return writeFloat(w, value.Float(), 64)
	case reflect.Slice:
		if value.Type().Elem().Kind() == reflect.Uint8 {
			return writeBytes(w, value.Bytes())
		}
	default:
		for _, typ := range convertibleTypes {
			if value.Type().ConvertibleTo(typ) {
				return d.WriteArgument(w, value.Convert(typ).Interface())
			}
		}
	}
	return fmt.Errorf("unsupported argument type %T", arg)
}

func writeFloat(w dbs.Writer, value float64, bitSize int) error {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return fmt.Errorf("unsupported float value: %v", value)
	}
	return writeString(w, strconv.FormatFloat(value, 'f', -1, bitSize))
}

func writeTime(w dbs.Writer, value time.Time) (err error) {
	if err = w.WriteByte('\''); err != nil {
		return err
	}
	if value.IsZero() {
		if _, err = w.WriteString("0000-00-00 00:00:00"); err != nil {
			return err
		}
	} else {
		if _, err = w.WriteString(value.Format("2006-01-02 15:04:05.999999")); err != nil {
			return err
		}
	}
	return w.WriteByte('\'')
}

func writeQuotedString(w dbs.Writer, value string) (err error) {
	if err = w.WriteByte('\''); err != nil {
		return err
	}
	for i := 0; i < len(value); i++ {
		switch value[i] {
		case '\\':
			if _, err = w.WriteString(`\\`); err != nil {
				return err
			}
		case '\'':
			if _, err = w.WriteString(`\'`); err != nil {
				return err
			}
		case '\x00':
			if _, err = w.WriteString(`\0`); err != nil {
				return err
			}
		case '\n':
			if _, err = w.WriteString(`\n`); err != nil {
				return err
			}
		case '\r':
			if _, err = w.WriteString(`\r`); err != nil {
				return err
			}
		case '\t':
			if _, err = w.WriteString(`\t`); err != nil {
				return err
			}
		case '\b':
			if _, err = w.WriteString(`\b`); err != nil {
				return err
			}
		case '\x1a':
			if _, err = w.WriteString(`\Z`); err != nil {
				return err
			}
		default:
			if err = w.WriteByte(value[i]); err != nil {
				return err
			}
		}
	}
	return w.WriteByte('\'')
}

func writeBytes(w dbs.Writer, value []byte) (err error) {
	if _, err = w.WriteString("X'"); err != nil {
		return err
	}

	const hex = "0123456789ABCDEF"

	for _, b := range value {
		if err = w.WriteByte(hex[b>>4]); err != nil {
			return err
		}
		if err = w.WriteByte(hex[b&0x0f]); err != nil {
			return err
		}
	}
	return w.WriteByte('\'')
}

func writeString(w dbs.Writer, value string) error {
	_, err := w.WriteString(value)
	return err
}
