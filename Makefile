PACKAGE_NAME         := github.com/allegro/akubra/akubra
VERSION_PACKAGE_NAME :=$(PACKAGE_NAME)/version
VERSION				 := `./get_version.sh`

all: lint test build

lint: deps-lint
	gometalinter ./... \
	--disable=gotype \
	--disable=dupl \
	--deadline=60s \
	--enable=goimports \
	--vendor

deps:
	glide install

deps-lint: deps
	go get github.com/alecthomas/gometalinter
	gometalinter --install

build: deps lint
	go build .

test: deps
	go test -v -race -cover $$(go list ./... | grep -v /vendor/)

clean:
	go clean .