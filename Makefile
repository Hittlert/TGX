.PHONY: all build test clean docker

BINARY_NAME=tgx

all: build

build:
	go build -o $(BINARY_NAME) .

test:
	go test ./pkg/...

clean:
	rm -f $(BINARY_NAME)

docker:
	docker build -t hittlert/tgx:latest .
