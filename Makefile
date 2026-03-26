SHELL := /bin/sh

BINARY := bin/snsd
IMAGE ?= emulator-aws-sns:latest
PORT ?= 4100
AWS_REGION ?= us-east-1
AWS_ACCOUNT_ID ?= 123456789012

.PHONY: build run test test-unit test-integration test-sdk test-cli fmt lint vet clean docker-build docker-run test-race coverage ci

build:
	mkdir -p bin
	go build -o $(BINARY) ./cmd/snsd

run:
	SNS_ADDR=:$(PORT) AWS_REGION=$(AWS_REGION) AWS_ACCOUNT_ID=$(AWS_ACCOUNT_ID) go run ./cmd/snsd

test: test-unit

test-unit:
	go test ./...

test-integration:
	go test -tags=integration ./internal/integration -v

test-sdk:
	go test -tags=integration ./internal/integration -run TestSDK -v

test-cli:
	./test/cli/run.sh

fmt:
	gofmt -w $$(find . -name '*.go' -type f)

lint:
	@command -v golangci-lint >/dev/null 2>&1 || { echo "golangci-lint is required for the lint target"; exit 1; }
	golangci-lint run ./...

vet:
	go vet ./...

test-race:
	go test -race ./...

coverage:
	go test -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out

docker-build:
	docker build -t $(IMAGE) .

docker-run:
	docker run --rm -p $(PORT):4100 \
		-e SNS_ADDR=:4100 \
		-e AWS_REGION=$(AWS_REGION) \
		-e AWS_ACCOUNT_ID=$(AWS_ACCOUNT_ID) \
		$(IMAGE)

clean:
	rm -rf bin coverage.out

ci: fmt vet test-unit test-integration
