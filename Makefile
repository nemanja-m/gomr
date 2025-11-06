.PHONY: genproto build build-local build-coordinator tools test clean run help

BUILD_DIR=./bin
CMD_DIR=./cmd

PROTOC_GEN_GO_VERSION      ?= v1.36.10
PROTOC_GEN_GO_GRPC_VERSION ?= v1.5.1

help:
	@echo "Available targets:"
	@echo "  build              - Build all binaries"
	@echo "  build-local        - Build the local binary"
	@echo "  build-coordinator  - Build the coordinator binary"
	@echo "  genproto           - Generate protobuf code"
	@echo "  tools              - Install required protobuf tools"
	@echo "  test               - Run all tests"
	@echo "  clean              - Remove build artifacts"
	@echo "  run                - Run the local binary (use ARGS variable for arguments)"

build: build-local build-coordinator

build-local:
	@mkdir -p $(BUILD_DIR)
	go build -o $(BUILD_DIR)/local $(CMD_DIR)/local

build-coordinator: genproto
	@mkdir -p $(BUILD_DIR)
	go build -o $(BUILD_DIR)/coordinator $(CMD_DIR)/coordinator

genproto: tools
	protoc \
		--go_out=. \
		--go_opt=paths=source_relative \
		--go-grpc_out=. \
		--go-grpc_opt=paths=source_relative \
		internal/shared/proto/*.proto

tools:
	@if ! command -v protoc-gen-go >/dev/null 2>&1; then \
		echo "Installing protoc-gen-go..." \
		go install google.golang.org/protobuf/cmd/protoc-gen-go@$(PROTOC_GEN_GO_VERSION); \
	fi
	@if ! command -v protoc-gen-go-grpc >/dev/null 2>&1; then \
		echo "Installing protoc-gen-go-grpc..." \
		go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@$(PROTOC_GEN_GO_GRPC_VERSION); \
	fi

test:
	go test ./...

clean:
	rm -rf $(BUILD_DIR)
	rm -rf examples/data/output/

run: build-local
	$(BUILD_DIR)/local $(ARGS)
