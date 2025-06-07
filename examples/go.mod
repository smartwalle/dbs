module github.com/smartwalle/dbs/examples

require (
	github.com/jackc/pgx/v5 v5.5.5
	github.com/lib/pq v1.10.9
	github.com/smartwalle/dbs v1.2.5
)

require (
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20221227161230-091c0ba34f0a // indirect
	github.com/jackc/puddle/v2 v2.2.1 // indirect
	golang.org/x/crypto v0.35.0 // indirect
	golang.org/x/sync v0.11.0 // indirect
	golang.org/x/text v0.22.0 // indirect
)

replace github.com/smartwalle/dbs => ../

go 1.23.0
