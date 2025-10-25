.PHONY: build build-local build-coordinator test clean run help

BUILD_DIR=./bin
CMD_DIR=./cmd

help:
	@echo "Available targets:"
	@echo "  build              - Build all binaries"
	@echo "  build-local        - Build the local binary"
	@echo "  build-coordinator  - Build the coordinator binary"
	@echo "  test               - Run all tests"
	@echo "  clean              - Remove build artifacts"
	@echo "  run                - Run the local binary (use ARGS variable for arguments)"

build: build-local build-coordinator

build-local:
	@mkdir -p $(BUILD_DIR)
	go build -o $(BUILD_DIR)/local $(CMD_DIR)/local

build-coordinator:
	@mkdir -p $(BUILD_DIR)
	go build -o $(BUILD_DIR)/coordinator $(CMD_DIR)/coordinator

test:
	go test ./...

clean:
	rm -rf $(BUILD_DIR)
	rm -rf examples/data/output/

run: build-local
	$(BUILD_DIR)/local $(ARGS)
