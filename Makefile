.PHONY: all build test eval-self-test clean docker

BINARY_NAME=tgx

all: build

build:
	go build -o $(BINARY_NAME) .

test:
	go test ./pkg/...

eval-self-test:
	python3 scripts/evaluation/run_protocol_v1.py self-test

clean:
	rm -f $(BINARY_NAME)

docker:
	docker build -t hittlert/tgx:latest .
