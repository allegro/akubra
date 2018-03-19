package storages

import (
	"net/http"
	"fmt"
	"github.com/allegro/akubra/log"
)

const (
	InternalHostHeader   = "X-Akubra-Internal-Host-3yeLjyjQNx"
	InternalBucketHeader = "X-Akubra-Internal-Bucket-lejK0EpVZy"
	PathStyleFormat      = "/%s%s"
)

type domainStyleInterceptor struct {
	roundTripper http.RoundTripper
}

var domainStyleDecorator = func(roundTripper http.RoundTripper) http.RoundTripper {
	return &domainStyleInterceptor{roundTripper}
}

func (interceptor *domainStyleInterceptor) RoundTrip(req *http.Request) (*http.Response, error) {
	host := req.Header.Get(InternalHostHeader)
	bucket := req.Header.Get(InternalBucketHeader)

	if host == "" {
		return nil, fmt.Errorf("Missing host header, request id %d!", req.Context().Value(log.ContextreqIDKey))
	}

	if bucket != "" {
		req.URL.Path = fmt.Sprintf(PathStyleFormat, bucket, req.URL.Path)
		log.Debugf("Rewritten domain style url to path style url for request ", req.Context().Value(log.ContextreqIDKey))
	}

	req.Host = host
	req.URL.Host = host
	req.Header.Del(InternalHostHeader)
	req.Header.Del(InternalBucketHeader)
	return interceptor.roundTripper.RoundTrip(req)
}