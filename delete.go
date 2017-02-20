package dba

import (
	"strings"
	"fmt"
	"database/sql"
)

func Delete(s StmtPrepare, tblName string, where *Where) (sql.Result, error) {
	var query, values = BuildDeleteStmt(tblName, where)
	return Exec(s, query, values...)
}

func BuildDeleteStmt(tblName string, where *Where) (query string, values []interface{}) {
	var newTblName = strings.TrimSpace(tblName)

	values = make([]interface{}, 0, 0)

	var wc string
	var wq string
	if where != nil && strings.TrimSpace(where.condition) != "" {
		wq = "WHERE "
		wc = where.condition
		values = append(values, where.args...)
	}

	query = fmt.Sprintf("DELETE FROM %s %s%s", newTblName, wq, wc)

	return query, values
}
