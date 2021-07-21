GO := go
GO_TEST := $(GO) test -v -race -timeout 20m

PKGS := $(shell go list ./... | grep -v vendor)

.PHONY: all
all: test lint

.PHONY: lint
lint:
	golangci-lint run

.PHONY: test
test:
	$(GO_TEST) $(PKGS)

.PHONY: test_%
test_%:
	$(GO_TEST) -run ^$*$$ $(PKGS)
