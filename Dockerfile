# Build stage
FROM golang:1.23-alpine AS builder

WORKDIR /app

# Install dependencies
RUN apk add --no-cache git

# Copy Go module files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build binary
RUN CGO_ENABLED=0 GOOS=linux go build -o /bin/scraper ./cmd/scraper/

# Runtime stage
FROM alpine:3.19

WORKDIR /app

# Install CA certificates for HTTPS
RUN apk add --no-cache ca-certificates tzdata

# Copy binary from builder
COPY --from=builder /bin/scraper /app/scraper
COPY log-config.json /app/log-config.json
COPY config.yaml /app/config.yaml

# Copy SQL migrations
COPY --from=builder /app/internal/storage/migrations /app/internal/storage/migrations

# Create log directory
RUN mkdir -p /var/log/binance-scraper

# Set timezone
ENV TZ=Asia/Singapore

# Run as non-root user
RUN adduser -D -u 1000 appuser && chown -R appuser:appuser /app
USER appuser

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD wget --no-verbose --tries=1 --spider http://localhost:8080/health || exit 1

ENTRYPOINT ["/app/scraper"]
