package dba

import (
	"fmt"
	"strings"
	"database/sql"
)

func Insert(s StmtPrepare, tblName string, data map[string]interface{}) (sql.Result, error) {
	var query, values = BuildInsertStmt(tblName, data)
	return Exec(s, query, values...)
}

func BuildInsertStmt(tblName string, data map[string]interface{}) (query string, values []interface{}) {
	var newTblName = strings.TrimSpace(tblName)

	values = make([]interface{}, 0, 0)

	var keys = make([]string, 0, 0)
	var params = make([]string, 0, 0)
	for key, value := range data {
		keys = append(keys, key)
		params = append(params, "?")
		values = append(values, value)
	}

	query = fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", newTblName, strings.Join(keys, ", "), strings.Join(params, ", "))

	return query, values
}