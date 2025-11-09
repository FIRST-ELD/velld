FROM golang:1.24-alpine AS builder

WORKDIR /app

RUN apk add --no-cache gcc musl-dev

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=1 GOOS=linux go build -o main cmd/api-server/main.go

FROM alpine:latest

# Install database client tools
# PostgreSQL 16 client (try version 16, fallback to default)
RUN apk add --no-cache sqlite-libs && \
    (apk add --no-cache --repository=http://dl-cdn.alpinelinux.org/alpine/v3.19/main postgresql16-client 2>/dev/null || \
     apk add --no-cache --repository=http://dl-cdn.alpinelinux.org/alpine/edge/main postgresql16-client 2>/dev/null || \
     apk add --no-cache postgresql-client) && \
    apk add --no-cache \
    mysql-client \
    mongodb-tools \
    redis

WORKDIR /app

COPY --from=builder /app/main .
COPY --from=builder /app/internal/database ./internal/database

EXPOSE 8080

CMD ["./main"]
