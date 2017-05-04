VERSION := `cat VERSION`
LDFLAGS := -X main.version=$(VERSION)

all: lint test build

lint: deps-lint
	gometalinter ./... \
	--disable=gotype \
	--disable=dupl \
	--deadline=600s \
	--enable=goimports \
	--vendor \
        --fast

deps:
	go get github.com/Masterminds/glide
	glide install

deps-lint: deps
	go get github.com/alecthomas/gometalinter
	gometalinter --install

build: deps lint
        # Enable netcgo, then name resolution will use systems dns caches
	go build -v -ldflags "$(LDFLAGS)" -tags 'netcgo=1'.

test: deps
	go test -v -race -cover $$(go list ./... | grep -v /vendor/)

clean:
	go clean .
