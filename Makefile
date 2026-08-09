.PHONY: help build vet fmt fmt-check test test-race cover test-hardhat test-hardhat-race test-all check

HARDHAT_RPC_URL ?= http://127.0.0.1:8545

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

build: ## Compile the package
	go build ./...

vet: ## Run go vet
	go vet ./...

fmt: ## Reformat all source files with gofmt
	gofmt -w .

fmt-check: ## Fail if any file isn't gofmt-formatted
	@files="$$(gofmt -l .)"; \
	if [ -n "$$files" ]; then \
		echo "gofmt needed on:"; echo "$$files"; exit 1; \
	fi

test: ## Run the fast test suite (fakes only, no external services)
	go test ./...

test-race: ## Run the fast test suite with the race detector
	go test ./... -race

cover: ## Print per-function coverage of the idx package (fast suite only)
	go test ./tests/... -coverpkg=./... -coverprofile=/tmp/go-block-walk.cover
	go tool cover -func=/tmp/go-block-walk.cover

# Requires a Hardhat node already running (e.g. `npx hardhat node`) — this
# target does NOT start one itself. Point HARDHAT_RPC_URL elsewhere if it's
# not on the default http://127.0.0.1:8545. Tests skip (not fail) if no
# node is reachable.
test-hardhat: ## Run the opt-in tests against a real, already-running Hardhat node
	HARDHAT_RPC_URL=$(HARDHAT_RPC_URL) go test -tags hardhat ./tests/... -run TestHardhat -v

test-hardhat-race: ## test-hardhat, with the race detector
	HARDHAT_RPC_URL=$(HARDHAT_RPC_URL) go test -tags hardhat ./tests/... -run TestHardhat -race -v

test-all: test-race test-hardhat-race ## Fast suite + Hardhat suite, both with -race

check: build vet fmt-check test-race ## Everything CI should run before merging
