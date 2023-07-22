module github.com/smartwalle/dbs/examples

require (
	github.com/go-sql-driver/mysql v1.7.1
	github.com/lib/pq v1.10.9
	github.com/smartwalle/dbs v0.0.0
)

require (
	github.com/smartwalle/dbc v0.0.19 // indirect
	github.com/smartwalle/nsync v0.0.6 // indirect
	github.com/smartwalle/queue v0.0.4 // indirect
)

replace github.com/smartwalle/dbs => ../

go 1.18
