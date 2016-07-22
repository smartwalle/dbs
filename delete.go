package dba

import (
	"strings"
	"fmt"
	"database/sql"
)

func Delete(db StmtPrepare, tblName string, where *Where) (sql.Result, error) {
	var query, values = BuildDeleteStmt(tblName, where)

	stmt, err := db.Prepare(query)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	result, err := stmt.Exec(values...)
	return result, err
}

func BuildDeleteStmt(tblName string, where *Where) (query string, values []interface{}) {
	var newTblName = strings.TrimSpace(tblName)

	query = fmt.Sprintf("DELETE FROM %s WHERE %s", newTblName, where.condition)

	values = make([]interface{}, 0, 0)
	values = append(values, where.args...)

	return query, values
}