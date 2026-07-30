# ============================================================
# K8s Pod WebTerminal Gateway — Dockerfile
# ============================================================
# Multi-stage build: compile Go binary, package into minimal image.

# --- Stage 1: Build ---
FROM golang:1.22-alpine AS builder

RUN apk add --no-cache git ca-certificates

WORKDIR /build

# Cache dependency downloads.
COPY go.mod go.sum ./
RUN go mod download

# Copy source and build.
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o webterminal-gateway .

# --- Stage 2: Runtime ---
FROM alpine:3.20

RUN apk add --no-cache ca-certificates tzdata && \
    cp /usr/share/zoneinfo/Asia/Shanghai /etc/localtime && \
    echo "Asia/Shanghai" > /etc/timezone

# Create non-root user.
RUN addgroup -g 1000 appgroup && \
    adduser -u 1000 -G appgroup -s /bin/sh -D appuser

WORKDIR /app

# Copy binary and static files.
COPY --from=builder /build/webterminal-gateway .
COPY --from=builder /build/static ./static

# Use non-root user.
USER appuser

EXPOSE 8080

# Health check.
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD wget --no-verbose --tries=1 --spider http://localhost:8080/healthz || exit 1

ENTRYPOINT ["./webterminal-gateway"]
CMD ["--port=8080", "--static-dir=./static"]
