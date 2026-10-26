FROM golang:1.22-alpine AS builder

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY cmd ./cmd
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/function ./cmd/function

FROM alpine:3.20
RUN adduser -D -g '' appuser
USER appuser
WORKDIR /app
COPY --from=builder /out/function /app/function

ENV PORT=8080
EXPOSE 8080

ENTRYPOINT ["/app/function"]
