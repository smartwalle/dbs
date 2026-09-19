package postgres

import (
	"database/sql/driver"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"time"

	"github.com/smartwalle/dbs"
)

var _dialect = &dialect{
	location: time.Local,
}

const (
	kPlaceholder = '$'
)

func Dialect() dbs.Dialect {
	return _dialect
}

type dialect struct {
	location *time.Location
}

func (d *dialect) UseTimeLocation(location *time.Location) {
	if location == nil {
		location = time.Local
	}
	d.location = location
}

func (d *dialect) WriteIdentifier(w dbs.Writer, s string) (err error) {
	if s == "" {
		return nil
	}

	if err = w.WriteByte('"'); err != nil {
		return err
	}

	var start = 0
	for i := 0; i < len(s); i++ {
		if s[i] != '"' {
			continue
		}
		if start < i {
			if _, err = w.WriteString(s[start:i]); err != nil {
				return err
			}
		}
		if _, err = w.WriteString(`""`); err != nil {
			return err
		}
		start = i + 1
	}
	if start < len(s) {
		if _, err = w.WriteString(s[start:]); err != nil {
			return err
		}
	}
	return w.WriteByte('"')
}

func (d *dialect) WritePlaceholder(w dbs.Writer, idx int) (err error) {
	if err = w.WriteByte(kPlaceholder); err != nil {
		return err
	}
	if _, err = w.WriteString(strconv.Itoa(idx)); err != nil {
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
	case dbs.ExplainValuer:
		v, err := raw.ExplainValue()
		if err != nil {
			return err
		}
		return writeQuotedString(w, v)
	case driver.Valuer:
		v, err := raw.Value()
		if err != nil {
			return err
		}
		return d.WriteArgument(w, v)
	case time.Time:
		return d.writeTime(w, raw)
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
	case uintptr:
		return writeString(w, strconv.FormatUint(uint64(raw), 10))
	case float32:
		return writeFloat(w, float64(raw), 32)
	case float64:
		return writeFloat(w, raw, 64)
	case fmt.Stringer:
		return writeQuotedString(w, raw.String())
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
		reflect.Uint64,
		reflect.Uintptr:
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

func (d *dialect) writeTime(w dbs.Writer, value time.Time) (err error) {
	if err = w.WriteByte('\''); err != nil {
		return err
	}
	if _, err = w.WriteString(value.In(d.location).Format("2006-01-02 15:04:05.999999Z07:00")); err != nil {
		return err
	}
	return w.WriteByte('\'')
}

func writeFloat(w dbs.Writer, value float64, bitSize int) error {
	switch {
	case math.IsNaN(value):
		return writeString(w, "'NaN'")

	case math.IsInf(value, 1):
		return writeString(w, "'Infinity'")

	case math.IsInf(value, -1):
		return writeString(w, "'-Infinity'")
	default:
	}
	return writeString(w, strconv.FormatFloat(value, 'f', -1, bitSize))
}

func writeQuotedString(w dbs.Writer, value string) (err error) {
	if err = w.WriteByte('\''); err != nil {
		return err
	}
	for i := 0; i < len(value); i++ {
		switch value[i] {
		case '\'':
			if err = w.WriteByte('\''); err != nil {
				return err
			}

			if err = w.WriteByte('\''); err != nil {
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
	if _, err = w.WriteString(`'\x`); err != nil {
		return err
	}

	const hex = "0123456789abcdef"

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
