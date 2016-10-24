PACKAGE_NAME         := github.com/allegro/akubra/akubra
VERSION_PACKAGE_NAME :=$(PACKAGE_NAME)/version
VERSION				 := `./get_version.sh`

ifdef bamboo_agentId
    $(info Building on Bamboo, overwriting GOPATH)
    GOPATH         := ${CURDIR}/../../../..
    GO             := GOPATH="${GOPATH}" go
    SCM_REPOSITORY := ${bamboo_planRepository_repositoryUrl}
    SCM_COMMIT     := ${bamboo_planRepository_revision}
    SCM_BRANCH     := ${bamboo_planRepository_branchName}
    BUILT_DATE     := ${bamboo_buildTimeStamp}
    ifdef bamboo_ManualBuildTriggerReason_userName
        BUILT_BY   := ${bamboo_ManualBuildTriggerReason_userName}
    endif
else
    GO             := go
    SCM_REPOSITORY := `git ls-remote --get-url`
    SCM_COMMIT     := `git rev-parse HEAD`
    SCM_BRANCH     := `git rev-parse --abbrev-ref HEAD`
    BUILT_DATE     := `date +%FT%T%z`
endif

BUILT_HOST := `hostname`
BUILT_BY   ?= ${USER}

all: test build

lint: deps-lint
	@echo "Linting source code"
	${GOPATH}/bin/gometalinter ${CURDIR}/... \
	--disable=gotype \
	--disable=dupl \
	--deadline=60s \
	--vendor

deps:
	go get github.com/jstemmer/go-junit-report
	glide install

deps-lint: deps
	go get github.com/alecthomas/gometalinter
	${GOPATH}/bin/gometalinter --install

build: deps lint
	@echo "Building application"
	go build -ldflags \
	 "-X $(VERSION_PACKAGE_NAME).CurrentVersion=$(VERSION) \
	 -X $(VERSION_PACKAGE_NAME).SCMRepository=$(SCM_REPOSITORY) \
	 -X $(VERSION_PACKAGE_NAME).SCMCommit=$(SCM_COMMIT) \
	 -X $(VERSION_PACKAGE_NAME).SCMBranch=$(SCM_BRANCH) \
	 -X $(VERSION_PACKAGE_NAME).BuiltDate=$(BUILT_DATE) \
	 -X $(VERSION_PACKAGE_NAME).BuiltBy=$(BUILT_BY) \
	 -X $(VERSION_PACKAGE_NAME).BuiltHost=$(BUILT_HOST)" \
	 .


test: deps
	go test -v -race $$(go list ./... | grep -v /vendor/) |  go-junit-report > report.xml

test-nodeps:
	go test -v $(PACKAGE_NAME)/...

clean:
	@echo "Cleaning go and tycho files"
	@${GO} clean $(PACKAGE_NAME)
	-@rm -Rf build
