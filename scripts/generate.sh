#!/bin/sh
# Regenerate the protobuf and gRPC Go code.
#
# This runs inside a Linux container (see `make generate`) rather than on the
# host. The plugins are built from the versions pinned in go.mod, so codegen is
# reproducible without installing buf or protoc anywhere, and without depending
# on which binaries a given machine's execution policy happens to permit.
#
# The output is committed, so CI never runs this.
set -eu

TOOLS=/tmp/murmur-tools
mkdir -p "$TOOLS"

echo "building codegen plugins from go.mod versions"
go build -o "$TOOLS/buf" github.com/bufbuild/buf/cmd/buf
go build -o "$TOOLS/protoc-gen-go" google.golang.org/protobuf/cmd/protoc-gen-go
go build -o "$TOOLS/protoc-gen-go-grpc" google.golang.org/grpc/cmd/protoc-gen-go-grpc

PATH="$TOOLS:$PATH"
export PATH

echo "buf $("$TOOLS/buf" --version)"

echo "linting the schema"
buf lint

echo "generating"
rm -rf api/gen
buf generate

echo "generated:"
find api/gen -name '*.go' | sort | sed 's/^/  /'
