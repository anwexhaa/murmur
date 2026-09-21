package social_test

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/anwexhaa/murmur/internal/social"
)

// testPool is the package-wide connection pool, opened against a throwaway
// Postgres container in TestMain.
//
// One container for the package rather than one per test: starting Postgres
// costs seconds and truncating costs milliseconds. Tests get isolation from
// resetStore, not from a fresh container each.
var testPool *pgxpool.Pool

func TestMain(m *testing.M) {
	// testing.Short() reads a flag, so it is not meaningful until the flags
	// are parsed. m.Run would do it, but by then the container would already
	// have been started.
	flag.Parse()
	if testing.Short() {
		os.Exit(m.Run())
	}

	ctx := context.Background()

	// Two ways to get a database, and the tests cannot tell them apart.
	//
	// MURMUR_TEST_POSTGRES_DSN points at one that already exists. That is the
	// local path: the compose stack is already up, reusing it saves a
	// container start on every run, and it works on machines where spawning a
	// container from inside the test process does not.
	//
	// With no DSN, testcontainers starts a throwaway Postgres. That is the CI
	// path, and the one that guarantees a clean database nobody has touched.
	dsn := os.Getenv("MURMUR_TEST_POSTGRES_DSN")

	var container *tcpostgres.PostgresContainer
	if dsn == "" {
		var err error
		container, err = tcpostgres.Run(ctx, "postgres:16-alpine",
			tcpostgres.WithDatabase("murmur"),
			tcpostgres.WithUsername("murmur"),
			tcpostgres.WithPassword("murmur"),
			testcontainers.WithWaitStrategy(
				wait.ForLog("database system is ready to accept connections").
					WithOccurrence(2).
					WithStartupTimeout(2*time.Minute),
			),
		)
		if err != nil {
			log.Fatalf("start postgres container: %v", err)
		}
		if dsn, err = container.ConnectionString(ctx, "sslmode=disable"); err != nil {
			log.Fatalf("connection string: %v", err)
		}
	}

	code := run(ctx, dsn, m)

	if container != nil {
		if err := testcontainers.TerminateContainer(container); err != nil {
			log.Printf("terminate postgres container: %v", err)
		}
	}
	os.Exit(code)
}

// run holds the teardown that os.Exit would otherwise skip: a leaked container
// outlives the test binary, and a leaked pool holds connections open.
func run(ctx context.Context, dsn string, m *testing.M) int {
	if err := migrate(dsn); err != nil {
		log.Printf("migrate: %v", err)
		return 1
	}

	var err error
	testPool, err = pgxpool.New(ctx, dsn)
	if err != nil {
		log.Printf("open pool: %v", err)
		return 1
	}
	defer testPool.Close()

	return m.Run()
}

// migrate applies the real migrations, not a hand-written test schema. A test
// schema that drifts from the migrations tests a database that does not exist.
func migrate(dsn string) error {
	database, err := sql.Open("pgx", dsn)
	if err != nil {
		return fmt.Errorf("open: %w", err)
	}
	defer database.Close()

	goose.SetLogger(goose.NopLogger())
	if err := goose.SetDialect("postgres"); err != nil {
		return fmt.Errorf("dialect: %w", err)
	}
	if err := goose.Up(database, "../../migrations"); err != nil {
		return fmt.Errorf("up: %w", err)
	}
	return nil
}

// newStore returns a store over an empty database. Every test starts from the
// same known state.
func newStore(t *testing.T) *social.Store {
	t.Helper()
	requireDatabase(t)

	store := social.NewStore(testPool)
	if err := store.TruncateAll(t.Context()); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	return store
}

func requireDatabase(t *testing.T) {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping integration test: -short")
	}
	if testPool == nil {
		t.Fatal("no database: TestMain did not start one")
	}
}
