# Multi-stage Dockerfile for collaboration server
# Builder: compile static Go binaries
FROM golang:1.21-alpine AS builder
WORKDIR /src

RUN apk add --no-cache git build-base

# Cache deps
COPY go.mod go.sum ./
RUN go mod download

# Copy source
COPY . .

# Build server and small healthcheck (static)
ENV CGO_ENABLED=0 GOOS=linux GOARCH=amd64
RUN go build -ldflags='-s -w' -o /app/collab ./cmd/server
RUN go build -ldflags='-s -w' -o /app/healthcheck ./cmd/healthcheck

# Runtime: minimal, use distroless static image
FROM gcr.io/distroless/static-debian11
COPY --from=builder /app/collab /collab
COPY --from=builder /app/healthcheck /healthcheck

EXPOSE 8080

USER nonroot

ENTRYPOINT ["/collab"]

# Healthcheck runs the small helper that probes /health
HEALTHCHECK --interval=30s --timeout=5s CMD ["/healthcheck"]
