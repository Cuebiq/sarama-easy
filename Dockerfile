FROM golang:1.23.0 AS builder

WORKDIR /go/src/github.com/Cuebiq/sarama-easy

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /go/bin/ ./...

FROM gcr.io/distroless/static-debian12

COPY --from=builder /go/bin/ /usr/local/bin/

CMD ["echo", "use docker-compose up to run the examples"]
