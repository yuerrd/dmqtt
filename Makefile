.PHONY: build bench test lint clean run

BINARY=dmqtt
BUILD_DIR=bin

build:
	go build -o $(BUILD_DIR)/$(BINARY) ./cmd/dmqtt/

bench:
	go build -o $(BUILD_DIR)/dmqtt-bench ./cmd/dmqtt-bench/

test:
	go test -v -race -count=1 ./...

test-short:
	go test -short -race -count=1 ./...

lint:
	go vet ./...

clean:
	rm -rf $(BUILD_DIR)

run: build
	$(BUILD_DIR)/$(BINARY)
