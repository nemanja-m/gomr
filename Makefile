.PHONY: build test clean run help

BINARY_NAME=gomr
BUILD_DIR=./bin
CMD_DIR=./cmd/local

help:
	@echo "Available targets:"
	@echo "  build    - Build the gomr binary"
	@echo "  test     - Run all tests"
	@echo "  clean    - Remove build artifacts"
	@echo "  run      - Run the binary (use ARGS variable for arguments)"

build:
	go build -o $(BUILD_DIR)/$(BINARY_NAME) $(CMD_DIR)

test:
	go test ./...

clean:
	rm -f $(BUILD_DIR)/$(BINARY_NAME)
	rm -rf examples/data/output/

run: build
	$(BUILD_DIR)/$(BINARY_NAME) $(ARGS)
