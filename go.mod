module github.com/example/shard-router

go 1.22

replace gopkg.in/yaml.v3 => github.com/go-yaml/yaml/v3 v3.0.1

replace gopkg.in/check.v1 => github.com/go-check/check v0.0.0-20201130134442-10cb98267c6c

replace golang.org/x/crypto => github.com/golang/crypto v0.27.0

replace golang.org/x/text => github.com/golang/text v0.18.0

replace golang.org/x/sync => github.com/golang/sync v0.8.0

require github.com/jackc/pgx/v5 v5.7.1

require (
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	golang.org/x/crypto v0.27.0 // indirect
	golang.org/x/sync v0.8.0 // indirect
	golang.org/x/text v0.18.0 // indirect
)
