package dbs

import (
	"bytes"
	"database/sql/driver"
	"reflect"
	"strconv"
	"strings"
	"time"
)

var convertibleTypes = []reflect.Type{reflect.TypeOf(time.Time{})}

func Explain(clause Clause) (string, error) {
	sql, args, err := clause.SQL()
	if err != nil {
		return "", err
	}
	return ExplainSQL(sql, args)
}

func ExplainToBuffer(buffer *bytes.Buffer, clause Clause) (err error) {
	sql, args, err := clause.SQL()
	if err != nil {
		return err
	}
	return ExplainSQLToBuffer(buffer, sql, args)
}

func ExplainSQL(sql string, args []interface{}) (string, error) {
	var buffer = bytes.NewBuffer(make([]byte, 0, kDefaultBufferSize))
	if err := ExplainSQLToBuffer(buffer, sql, args); err != nil {
		return "", err
	}
	return buffer.String(), nil
}

func ExplainSQLToBuffer(buffer *bytes.Buffer, sql string, args []interface{}) (err error) {
	for len(sql) > 0 && len(args) > 0 {
		var pos = strings.IndexByte(sql, '?')
		if pos == -1 {
			break
		}

		if _, err = buffer.WriteString(sql[:pos]); err != nil {
			return err
		}

		if err = explainArgument(buffer, args[0]); err != nil {
			return err
		}

		args = args[1:]
		sql = sql[pos+1:]
	}
	if len(sql) > 0 {
		if _, err = buffer.WriteString(sql); err != nil {
			return err
		}
	}
	return nil
}

func explainArgument(buffer *bytes.Buffer, arg interface{}) (err error) {
	switch raw := arg.(type) {
	case time.Time:
		if err = buffer.WriteByte('\''); err != nil {
			return err
		}
		if raw.IsZero() {
			if _, err = buffer.WriteString("0000-00-00 00:00:00"); err != nil {
				return err
			}
		} else {
			if _, err = buffer.WriteString(raw.Format("2006-01-02 15:04:05.999")); err != nil {
				return err
			}
		}
		if err = buffer.WriteByte('\''); err != nil {
			return err
		}
	case *time.Time:
		if raw != nil {
			if err = buffer.WriteByte('\''); err != nil {
				return err
			}
			if raw.IsZero() {
				if _, err = buffer.WriteString("0000-00-00 00:00:00"); err != nil {
					return err
				}
			} else {
				if _, err = buffer.WriteString(raw.Format("2006-01-02 15:04:05.999")); err != nil {
					return err
				}
			}
			if err = buffer.WriteByte('\''); err != nil {
				return err
			}
		} else {
			if _, err = buffer.WriteString("NULL"); err != nil {
				return err
			}
		}
	case driver.Valuer:
		var value = reflect.ValueOf(raw)
		if !value.IsValid() {
			if _, err = buffer.WriteString("NULL"); err != nil {
				return err
			}
		} else {
			rawValue, err := raw.Value()
			if err != nil {
				return err
			}
			return explainArgument(buffer, rawValue)
		}
	case bool:
		if _, err = buffer.WriteString(strconv.FormatBool(raw)); err != nil {
			return err
		}
	case string:
		if err = buffer.WriteByte('\''); err != nil {
			return err
		}
		if _, err = buffer.WriteString(strings.ReplaceAll(raw, "'", "''")); err != nil {
			return err
		}
		if err = buffer.WriteByte('\''); err != nil {
			return err
		}
	case int8:
		if _, err = buffer.WriteString(strconv.FormatInt(int64(raw), 10)); err != nil {
			return err
		}
	case int:
		if _, err = buffer.WriteString(strconv.FormatInt(int64(raw), 10)); err != nil {
			return err
		}
	case int16:
		if _, err = buffer.WriteString(strconv.FormatInt(int64(raw), 10)); err != nil {
			return err
		}
	case int32:
		if _, err = buffer.WriteString(strconv.FormatInt(int64(raw), 10)); err != nil {
			return err
		}
	case int64:
		if _, err = buffer.WriteString(strconv.FormatInt(raw, 10)); err != nil {
			return err
		}
	case uint8:
		if _, err = buffer.WriteString(strconv.FormatUint(uint64(raw), 10)); err != nil {
			return err
		}
	case uint:
		if _, err = buffer.WriteString(strconv.FormatUint(uint64(raw), 10)); err != nil {
			return err
		}
	case uint16:
		if _, err = buffer.WriteString(strconv.FormatUint(uint64(raw), 10)); err != nil {
			return err
		}
	case uint32:
		if _, err = buffer.WriteString(strconv.FormatUint(uint64(raw), 10)); err != nil {
			return err
		}
	case uint64:
		if _, err = buffer.WriteString(strconv.FormatUint(raw, 10)); err != nil {
			return err
		}
	case float32:
		if _, err = buffer.WriteString(strconv.FormatFloat(float64(raw), 'f', -1, 32)); err != nil {
			return err
		}
	case float64:
		if _, err = buffer.WriteString(strconv.FormatFloat(raw, 'f', -1, 64)); err != nil {
			return err
		}
	default:
		var value = reflect.ValueOf(raw)
		var kind = value.Kind()

		if !value.IsValid() {
			if _, err = buffer.WriteString("NULL"); err != nil {
				return err
			}
			return nil
		}

		switch kind {
		case reflect.Ptr:
			if value.IsNil() {
				if _, err = buffer.WriteString("NULL"); err != nil {
					return err
				}
				return nil
			}
			return explainArgument(buffer, reflect.Indirect(value).Interface())
		case reflect.Bool:
			return explainArgument(buffer, value.Bool())
		case reflect.String:
			return explainArgument(buffer, value.String())
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			return explainArgument(buffer, value.Int())
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			return explainArgument(buffer, value.Uint())
		case reflect.Float32, reflect.Float64:
			return explainArgument(buffer, value.Float())
		default:
			for _, rType := range convertibleTypes {
				if value.Type().ConvertibleTo(rType) {
					return explainArgument(buffer, value.Convert(rType).Interface())
				}
			}
			return explainArgument(buffer, value.String())
		}
	}
	return nil
}
