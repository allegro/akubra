package storages

import (
	"fmt"
	"net/http"

	"github.com/allegro/akubra/log"
	"github.com/allegro/akubra/utils"
)

type domainStyleInterceptor struct {
	roundTripper http.RoundTripper
}

var domainStyleDecorator = func(roundTripper http.RoundTripper) http.RoundTripper {
	return &domainStyleInterceptor{roundTripper}
}

func (interceptor *domainStyleInterceptor) RoundTrip(req *http.Request) (*http.Response, error) {
	host := req.Header.Get(utils.InternalHostHeader)
	bucket := req.Header.Get(utils.InternalBucketHeader)
	if host == "" {
		return nil, fmt.Errorf("Missing host header, request id %d", req.Context().Value(log.ContextreqIDKey))
	}
	if bucket != "" {
		pathStyleURL := fmt.Sprintf("/%s%s", bucket, req.URL.Path)
		log.Debugf("Rewritten domain style url (%s) to path style url (%s) for request %s",
		req.URL.Path, pathStyleURL, req.Context().Value(log.ContextreqIDKey))
		req.URL.Path = pathStyleURL
	}
	req.Host = host
	req.URL.Host = host
	req.Header.Del(utils.InternalHostHeader)
	req.Header.Del(utils.InternalBucketHeader)
	return interceptor.roundTripper.RoundTrip(req)
}