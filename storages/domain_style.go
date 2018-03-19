package storages

import (
	"net/http"
	"fmt"
	"github.com/allegro/akubra/log"
)

const (
	HOST              = "X-3yeLjyjQNx"
	BUCKET            = "X-lejK0EpVZy"
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
		log.Debugf("Rewritten domain style url to path style url for request ", req.Context().Value(log.ContextreqIDKey))
	}

	req.Host = host
	req.URL.Host = host
	req.Header.Del(HOST)
	req.Header.Del(BUCKET)
	return interceptor.roundTripper.RoundTrip(req)
}