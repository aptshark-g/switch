FROM golang:1.23-alpine AS builder
WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o gateway ./cmd/gateway/

FROM alpine:3.20
RUN apk add --no-cache ca-certificates tzdata
RUN adduser -D -g '' gateway
USER gateway
WORKDIR /app
COPY --from=builder /build/gateway .
COPY provider.yaml .

HEALTHCHECK --interval=15s --timeout=5s --retries=3 \
  CMD wget -qO- http://localhost:8080/v1/health || exit 1

EXPOSE 8080
ENTRYPOINT ["./gateway"]
CMD ["-config", "provider.yaml", "-addr", ":8080", "-state", "/data/gateway.state.json"]
