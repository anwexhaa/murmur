package buildcheck

import (
	"go/format"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// moduleRoot is this package's directory walked back up to the module root.
const moduleRoot = "../.."

var skipDirs = map[string]bool{
	".git":   true,
	"bin":    true,
	"vendor": true,
}

// Formatting is enforced here rather than by a `gofmt -l` step in the
// Makefile, so that it holds on every machine that can run `go test` — which
// includes ones where the gofmt binary cannot be executed at all.
func TestEveryGoFileIsFormatted(t *testing.T) {
	var unformatted []string

	err := filepath.WalkDir(moduleRoot, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if skipDirs[entry.Name()] {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(entry.Name(), ".go") {
			return nil
		}

		source, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		formatted, err := format.Source(source)
		if err != nil {
			// A file that will not parse is a compile error, which the build
			// reports far better than this test could.
			return nil
		}
		if string(source) != string(formatted) {
			rel, relErr := filepath.Rel(moduleRoot, path)
			if relErr != nil {
				rel = path
			}
			unformatted = append(unformatted, filepath.ToSlash(rel))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walking the module: %v", err)
	}

	if len(unformatted) > 0 {
		t.Errorf("%d file(s) are not gofmt-clean; run `make fmt`:\n  %s",
			len(unformatted), strings.Join(unformatted, "\n  "))
	}
}
