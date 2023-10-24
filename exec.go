package dbs

import (
	"context"
	"database/sql"
)

var mapper = NewMapper(kTag)

func Scan(rows *sql.Rows, dst interface{}) (err error) {
	return mapper.Decode(rows, dst)
}

func scanContext(ctx context.Context, s Session, b Builder, dst interface{}) (err error) {
	//var tx Transaction
	//var prefix string
	//
	//if nTx, ok := s.(Transaction); ok {
	//	tx = nTx
	//	prefix = tx.String() + " " + b.Type()
	//} else {
	//	prefix = b.Type()
	//}
	//
	//defer func() {
	//	if err != nil && tx != nil {
	//		tx.rollback(5)
	//	}
	//}()

	sqlStr, args, err := b.SQL()
	if err != nil {
		//logger.Output(3, fmt.Sprintln(prefix, "Build Failed:", err))
		return err
	}
	//logger.Output(3, fmt.Sprintln(prefix, "Build Success:", sqlStr, args))
	rows, err := s.QueryContext(ctx, sqlStr, args...)
	if err != nil {
		//logger.Output(3, fmt.Sprintln(prefix, "Query Failed:", err))
		return err
	}
	defer rows.Close()

	if err = mapper.Decode(rows, dst); err != nil {
		//logger.Output(3, fmt.Sprintln(prefix, "Scan Failed:", err))
		return err
	}
	return nil
}

func scanRowContext(ctx context.Context, s Session, b Builder, dst ...interface{}) (err error) {
	//var tx Transaction
	//var prefix string
	//
	//if nTx, ok := s.(Transaction); ok {
	//	tx = nTx
	//	prefix = tx.String() + " " + b.Type()
	//} else {
	//	prefix = b.Type()
	//}
	//
	//defer func() {
	//	if err != nil && tx != nil {
	//		tx.rollback(5)
	//	}
	//}()

	sqlStr, args, err := b.SQL()
	if err != nil {
		//logger.Output(3, fmt.Sprintln(prefix, "Build Failed:", err))
		return err
	}
	//logger.Output(3, fmt.Sprintln(prefix, "Build Success:", sqlStr, args))
	rows, err := s.QueryContext(ctx, sqlStr, args...)
	if err != nil {
		//logger.Output(3, fmt.Sprintln(prefix, "Query Failed:", err))
		return err
	}
	defer rows.Close()

	//for _, dp := range dst {
	//	if _, ok := dp.(*sql.RawBytes); ok {
	//		err = errors.New("sql: RawBytes isn't allowed on Row.Scan")
	//		logger.Output(3, fmt.Sprintln(prefix, "Scan Failed:", err))
	//		return err
	//	}
	//}

	if !rows.Next() {
		if err = rows.Err(); err != nil {
			//logger.Output(3, fmt.Sprintln(prefix, "Scan Failed:", err))
			return err
		}
		//logger.Output(3, fmt.Sprintln(prefix, "Scan Failed:", sql.ErrNoRows))
		return sql.ErrNoRows
	}
	if err = rows.Scan(dst...); err != nil {
		//logger.Output(3, fmt.Sprintln(prefix, "Scan Failed:", err))
		return err
	}
	return rows.Close()
}

func queryContext(ctx context.Context, s Session, b Builder) (result *sql.Rows, err error) {
	//var tx Transaction
	//var prefix string
	//
	//if nTx, ok := s.(Transaction); ok {
	//	tx = nTx
	//	prefix = tx.String() + " " + b.Type()
	//} else {
	//	prefix = b.Type()
	//}
	//
	//defer func() {
	//	if err != nil && tx != nil {
	//		tx.rollback(5)
	//	}
	//}()

	sqlStr, args, err := b.SQL()
	if err != nil {
		//logger.Output(3, fmt.Sprintln(prefix, "Build Failed:", err))
		return nil, err
	}
	//logger.Output(3, fmt.Sprintln(prefix, "Build Success:", sqlStr, args))
	result, err = s.QueryContext(ctx, sqlStr, args...)
	//if err != nil {
	//	logger.Output(3, fmt.Sprintln(prefix, "Query Failed:", err))
	//}
	return result, err
}

func execContext(ctx context.Context, s Session, b Builder) (result sql.Result, err error) {
	//var tx Transaction
	//var prefix string
	//
	//if nTx, ok := s.(Transaction); ok {
	//	tx = nTx
	//	prefix = tx.String() + " " + b.Type()
	//} else {
	//	prefix = b.Type()
	//}
	//
	//defer func() {
	//	if err != nil && tx != nil {
	//		tx.rollback(5)
	//	}
	//}()

	sqlStr, args, err := b.SQL()
	if err != nil {
		//logger.Output(3, fmt.Sprintln(prefix, "Build Failed:", err))
		return nil, err
	}

	//logger.Output(3, fmt.Sprintln(prefix, "Build Success:", sqlStr, args))
	result, err = s.ExecContext(ctx, sqlStr, args...)
	//if err != nil {
	//	logger.Output(3, fmt.Sprintln(prefix, "Exec Failed:", err))
	//}
	return result, err
}
