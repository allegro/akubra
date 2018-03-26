package storages

import (
	"net/http"
	"github.com/allegro/akubra/utils"
)

type internalHeadersCleaner struct {
	roundTripper http.RoundTripper
}

func (internalHeadersCleaner *internalHeadersCleaner) RoundTrip(request *http.Request) (*http.Response, error) {
	request.Header.Del(utils.InternalBucketHeader)
	request.Header.Del(utils.InternalPathStyleFlag)
	return internalHeadersCleaner.roundTripper.RoundTrip(request)
}

var internalHeadersCleanerDecorator = func(roundTripper http.RoundTripper) http.RoundTripper {
	return &internalHeadersCleaner{roundTripper}
}