// Command migrate applies the SQL migrations in ./migrations.
//
// A dedicated binary rather than the goose CLI: this one links only the pgx
// driver, so the schema tool cannot drift from the driver the services use,
// and it deploys as an init container in Phase 8 without a second image.
//
// Usage:
//
//	migrate [-dsn ...] [-dir ...] up|down|status|version|redo|up-to <n>
//
// The migrations are compiled in. -dir overrides that with a directory on
// disk, which is only useful when writing one; the deployed image has no
// filesystem to read from.
package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"os"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"

	"github.com/anwexhaa/murmur/migrations"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "migrate: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	dsn := flag.String("dsn", os.Getenv("POSTGRES_DSN"), "postgres connection string (defaults to $POSTGRES_DSN)")
	dir := flag.String("dir", "", "read migrations from this directory instead of the embedded copy")
	flag.Parse()

	if *dsn == "" {
		return errors.New("no connection string: set POSTGRES_DSN or pass -dsn")
	}

	command := "up"
	var args []string
	if rest := flag.Args(); len(rest) > 0 {
		command, args = rest[0], rest[1:]
	}

	database, err := sql.Open("pgx", *dsn)
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer database.Close()

	if err := goose.SetDialect("postgres"); err != nil {
		return fmt.Errorf("set dialect: %w", err)
	}

	// The embedded copy by default, so the binary carries its own schema and
	// cannot be deployed without it.
	source := "."
	if *dir != "" {
		goose.SetBaseFS(nil)
		source = *dir
	} else {
		goose.SetBaseFS(migrations.FS)
	}

	if err := goose.RunContext(context.Background(), command, database, source, args...); err != nil {
		return fmt.Errorf("%s: %w", command, err)
	}
	return nil
}
