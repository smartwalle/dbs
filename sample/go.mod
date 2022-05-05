module github.com/smartwalle/dbs/sample

require (
	github.com/smartwalle/dbs v0.0.0
	github.com/smartwalle/dbc v0.0.11
	github.com/smartwalle/xid v1.0.6
	github.com/smartwalle/queue v0.0.1
)

replace (
	github.com/smartwalle/dbs => ../
)

go 1.18
