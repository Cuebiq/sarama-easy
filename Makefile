.PHONY: test lint vet build docker clean

test:
	go test -race -count=1 -v ./...

lint:
	golangci-lint run ./...

vet:
	go vet ./...

build:
	go build -o bin/ ./...

docker:
	docker build -t sarama-easy .

clean:
	rm -rf bin/
