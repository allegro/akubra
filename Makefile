VERSION := `cat VERSION`
LDFLAGS := -X main.version=$(VERSION)

all: lint test build

lint: deps-lint
	gometalinter ./... \
	--disable=gotype \
	--disable=dupl \
	--deadline=600s \
	--enable=goimports \
	--vendor

deps:
	go get github.com/Masterminds/glide
	glide install

deps-lint: deps
	go get github.com/alecthomas/gometalinter
	gometalinter --install

build: deps lint
	go build -v -ldflags "$(LDFLAGS)" .

test: deps
	go test -v -race -cover $$(go list ./... | grep -v /vendor/)

clean:
	go clean .
