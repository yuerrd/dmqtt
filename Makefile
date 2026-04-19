.PHONY: build build-admin bench example-plugin test lint clean run install docker-build

BINARY=dmqtt
BUILD_DIR=bin

build-admin:
	cd web/admin && npm install && npm run build
	rm -rf internal/httpapi/admin
	cp -r web/admin/dist internal/httpapi/admin

build: build-admin
	go build -o $(BUILD_DIR)/$(BINARY) ./cmd/dmqtt/

build-go:
	go build -o $(BUILD_DIR)/$(BINARY) ./cmd/dmqtt/

bench:
	go build -o $(BUILD_DIR)/dmqtt-bench ./cmd/dmqtt-bench/

example-plugin:
	go build -buildmode=plugin -o $(BUILD_DIR)/log-interceptor.so ./examples/plugins/loginterceptor/

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

install: build
	@echo "Installing DMQTT to /opt/dmqtt..."
	sudo ./deploy/bare-metal/install.sh

docker-build:
	docker build -f deploy/k8s/Dockerfile -t dmqtt:latest .
