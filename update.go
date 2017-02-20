package dba

import (
	"database/sql"
	"fmt"
	"strings"
)

func Update(s StmtPrepare, tblName string, data map[string]interface{}, where *Where) (sql.Result, error) {
	var query, values = BuildUpdateStmt(tblName, data, where)
	return Exec(s, query, values...)
}

func BuildUpdateStmt(tblName string, data map[string]interface{}, where *Where) (query string, values []interface{}) {
	var newTblName = strings.TrimSpace(tblName)

	values = make([]interface{}, 0, 0)

	var keys = make([]string, 0, 0)
	for key, value := range data {
		keys = append(keys, key+"=?")
		values = append(values, value)
	}

	var wc string
	var wq string
	if where != nil && strings.TrimSpace(where.condition) != "" {
		wq = "WHERE "
		wc = where.condition
		values = append(values, where.args...)
	}

	query = fmt.Sprintf("UPDATE %s SET %s %s%s", newTblName, strings.Join(keys, ", "), wq, wc)

	return query, values
}
