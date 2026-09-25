# One Dockerfile for all four binaries, selected by build argument.
#
# Four near-identical Dockerfiles would drift: one would get a flag the others
# missed, and the difference would surface as a production-only bug months
# later. The services differ in exactly one respect -- which main package they
# build -- so that is the only thing parameterised.

# ---------------------------------------------------------------- build
FROM golang:1.27 AS build

WORKDIR /src

# Dependencies first, in their own layer. They change when go.mod changes,
# which is rarely; the source changes on every commit. Copying them together
# would re-download the module graph for a one-line edit.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY . .

ARG SERVICE
ARG VERSION=dev
ARG COMMIT=unknown

# CGO_ENABLED=0 is what makes the static base below possible: the binary has
# no dynamic linker to find and no libc to link against.
#
# -trimpath strips the build machine's directory layout out of the binary,
# which is both smaller and one less thing disclosed by a stack trace.
# -s -w drop the symbol table and DWARF sections; the tradeoff is that a core
# dump needs the unstripped binary, which the build cache still holds.
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux go build \
      -trimpath \
      -ldflags="-s -w -X main.version=${VERSION} -X main.commit=${COMMIT}" \
      -o /out/service ./cmd/${SERVICE}

# ---------------------------------------------------------------- runtime
#
# distroless/static holds a CA bundle, timezone data, /etc/passwd and nothing
# else. No shell, no package manager, no busybox -- so a remote code execution
# lands somewhere with nothing to execute, and the image has no OS packages to
# generate CVE noise about.
#
# :nonroot runs as uid 65532. Kubernetes can enforce that with
# runAsNonRoot, but a container that is only non-root because the orchestrator
# said so is one misapplied manifest from running as root.
FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /out/service /service

# Documentation rather than configuration: publishing a port is the runtime's
# job, and these are the defaults each binary listens on.
EXPOSE 8080 9080

# No shell form. `ENTRYPOINT /service` would be wrapped in `/bin/sh -c`, which
# does not exist here, and would also make the binary PID 2 with a shell that
# ignores SIGTERM in front of it -- turning every graceful shutdown into a
# thirty-second wait and a kill.
ENTRYPOINT ["/service"]
