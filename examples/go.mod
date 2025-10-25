module github.com/smartwalle/dbs/examples

require (
	github.com/jackc/pgx/v5 v5.5.5
	github.com/lib/pq v1.10.9
	github.com/smartwalle/dbs v1.2.5
)

require (
	filippo.io/edwards25519 v1.1.0 // indirect
	github.com/go-sql-driver/mysql v1.9.2 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20221227161230-091c0ba34f0a // indirect
	github.com/jackc/puddle/v2 v2.2.1 // indirect
	golang.org/x/crypto v0.17.0 // indirect
	golang.org/x/sync v0.1.0 // indirect
	golang.org/x/text v0.14.0 // indirect
)

replace github.com/smartwalle/dbs => ../

go 1.21.0

toolchain go1.23.4
