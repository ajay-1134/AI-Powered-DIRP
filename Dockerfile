# Multi-stage build producing all six binaries in one small image. Each
# docker-compose service runs the same image with a different entrypoint.
FROM golang:1.25 AS build
WORKDIR /src

# Cache module downloads.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN set -e; \
    CGO_ENABLED=0 GOOS=linux go build -o /out/platform ./cmd/platform; \
    for s in user order payment ledger notification; do \
      CGO_ENABLED=0 GOOS=linux go build -o /out/$s ./cmd/services/$s; \
    done

# Distroless static image: no shell, minimal attack surface. The dashboard HTML
# is embedded in the platform binary via go:embed, so nothing else is needed.
FROM gcr.io/distroless/static-debian12
COPY --from=build /out/ /bin/
# Default to the platform; compose overrides per service.
ENTRYPOINT ["/bin/platform"]
