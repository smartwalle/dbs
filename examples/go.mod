module github.com/smartwalle/dbs/examples

require (
	github.com/go-sql-driver/mysql v1.7.1
	github.com/lib/pq v1.10.9
	github.com/smartwalle/dbs v1.2.5
)

replace github.com/smartwalle/dbs => ../

go 1.18
