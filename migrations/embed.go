// Package migrations holds Murmur's schema, embedded.
//
// The SQL lives here, where a reader expects to find it, and this one Go file
// makes the directory a package so the same files can be compiled into the
// migrate binary. The alternative -- copying the .sql somewhere importable --
// would be two copies of the schema that drift the first time somebody edits
// the wrong one.
//
// Embedding matters for the distroless image in particular: it holds one
// binary and nothing else, so a migrator that read its SQL off disk would
// start, find an empty directory, and report that the database is already up
// to date.
package migrations

import "embed"

// FS holds every migration.
//
//go:embed *.sql
var FS embed.FS
