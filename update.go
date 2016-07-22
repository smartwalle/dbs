package dba

import (
	"database/sql"
	"fmt"
	"strings"
)

func Update(db StmtPrepare, tblName string, update map[string]interface{}, where *Where) (sql.Result, error) {
	var query, values = BuildUpdateStmt(tblName, update, where)

	stmt, err := db.Prepare(query)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	result, err := stmt.Exec(values...)

	return result, err
}

func BuildUpdateStmt(tblName string, update map[string]interface{}, where *Where) (query string, values []interface{}) {
	var newTblName = strings.TrimSpace(tblName)

	values = make([]interface{}, 0, 0)

	var keys = make([]string, 0, 0)
	for key, value := range update {
		keys = append(keys, key+"=?")
		values = append(values, value)
	}

	var wc string
	var wq string
	if where != nil {
		wq = "WHERE "
		wc = where.condition
		values = append(values, where.args...)
	}

	query = fmt.Sprintf("UPDATE %s SET %s %s%s", newTblName, strings.Join(keys, ", "), wq, wc)

	return query, values
}
