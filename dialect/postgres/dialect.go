package postgres

import (
	"database/sql/driver"
	"fmt"
	"reflect"
	"strconv"
	"time"

	"github.com/smartwalle/dbs"
)

var _dialect = &dialect{}

const (
	kPlaceholder = '$'
)

func Dialect() dbs.Dialect {
	return _dialect
}

type dialect struct {
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
	switch raw := arg.(type) {
	case nil:
		return writeString(w, "NULL")
	case driver.Valuer:
		value, err := raw.Value()
		if err != nil {
			return err
		}
		return d.WriteArgument(w, value)
	case time.Time:
		return writeTime(w, raw)
	case *time.Time:
		if raw == nil {
			return writeString(w, "NULL")
		}
		return writeTime(w, *raw)
	case bool:
		return writeString(w, strconv.FormatBool(raw))
	case string:
		return writeQuotedString(w, raw)
	case []byte:
		return writeQuotedString(w, string(raw))
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
		return writeString(w, strconv.FormatFloat(float64(raw), 'f', -1, 32))
	case float64:
		return writeString(w, strconv.FormatFloat(raw, 'f', -1, 64))
	default:
		value := reflect.ValueOf(raw)
		if !value.IsValid() {
			return writeString(w, "NULL")
		}

		switch value.Kind() {
		case reflect.Ptr:
			if value.IsNil() {
				return writeString(w, "NULL")
			}
			return d.WriteArgument(w, value.Elem().Interface())
		case reflect.Bool:
			return d.WriteArgument(w, value.Bool())
		case reflect.String:
			return d.WriteArgument(w, value.String())
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			return d.WriteArgument(w, value.Int())
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			return d.WriteArgument(w, value.Uint())
		case reflect.Float32, reflect.Float64:
			return d.WriteArgument(w, value.Float())
		default:
			for _, typ := range convertibleTypes {
				if value.Type().ConvertibleTo(typ) {
					return d.WriteArgument(w, value.Convert(typ).Interface())
				}
			}
		}
		return fmt.Errorf("unsupported argument type %T", arg)
	}
}

func writeTime(w dbs.Writer, value time.Time) (err error) {
	if err = w.WriteByte('\''); err != nil {
		return err
	}
	if value.IsZero() {
		if _, err = w.WriteString("0001-01-01 00:00:00"); err != nil {
			return err
		}
	} else {
		if _, err = w.WriteString(value.Format("2006-01-02 15:04:05.999")); err != nil {
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
		case '\'':
			if _, err = w.WriteString("''"); err != nil {
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

func writeString(w dbs.Writer, value string) error {
	_, err := w.WriteString(value)
	return err
}
