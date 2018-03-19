package storages

import (
	"net/http"
	"fmt"
	"github.com/allegro/akubra/log"
)

const (
	HOST              = "X-Akubra-Internal-Host"
	BUCKET            = "X-Akubra-Internal-Bucket"
	PATH_STYLE_FORMAT = "/%s%s"
)

type domainStyleInterceptor struct {
	roundTripper http.RoundTripper
}

var domainStyleDecorator = func(roundTripper http.RoundTripper) http.RoundTripper {
	return &domainStyleInterceptor{roundTripper}
}

func (interceptor *domainStyleInterceptor) RoundTrip(req *http.Request) (*http.Response, error) {

	host := req.Header.Get(HOST)
	bucket := req.Header.Get(BUCKET)

	if host == "" {
		return nil, fmt.Errorf("Missing host header, request id %d!", req.Context().Value(log.ContextreqIDKey))
	}

	if bucket != "" {
		req.URL.Path = fmt.Sprintf(PATH_STYLE_FORMAT, bucket, req.URL.Path)
		req.Header.Del(BUCKET)
		log.Debugf("Rewritten domain style url to path style url for request ", req.Context().Value(log.ContextreqIDKey))

	}

	req.Host = host
	req.Header.Del(HOST)
	return interceptor.roundTripper.RoundTrip(req)
}