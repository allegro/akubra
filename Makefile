VERSION := `git log -n 1 | grep commit | sed 's/commit //g' | head -n 1`
LDFLAGS := -X main.version=$(VERSION)
GO := "$(GOROOT)/bin/go"

all: vars formatting lint test build

vars:
	@echo "====== Makefile internal variables:"
	@echo "VERSION: '$(VERSION)'"
	@echo "LDFLAGS: '$(LDFLAGS)'"
	@echo "GO: '$(GO)'"
	@echo "======\n\n"

linux: vars formatting lint test
	GOOS=linux $(GO) build -v -ldflags "$(LDFLAGS)" -tags 'netcgo=1'.

formatting:
	$(GO) get golang.org/x/tools/cmd/goimports

lint: deps-lint
	gometalinter ./... \
	--disable=gotype \
	--disable=dupl \
	--deadline=600s \
	--disable=goimports \
	--vendor \
	--fast

lint-slow: deps-lint
	gometalinter ./... \
	--disable=gotype \
	--disable=dupl \
	--deadline=600s \
	--enable=goimports \
	--vendor 

deps:
	$(GO) get github.com/Masterminds/glide
	glide install

deps-lint: deps
	$(GO) get github.com/alecthomas/gometalinter
	gometalinter --install

build: vars deps lint
        # Enable netcgo, then name resolution will use systems dns caches
	$(GO) build -v -ldflags "$(LDFLAGS)" -tags 'netcgo=1'.

test: deps
	$(GO) test -v -race -cover $$(go list ./... | grep -v /vendor/)

clean:
	$(GO) clean .
