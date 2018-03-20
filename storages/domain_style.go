package storages

import (
	"fmt"
	"net/http"

	"github.com/allegro/akubra/log"
)

const (
	// InternalHostHeader is used for rewriting domain style
	InternalHostHeader   = "X-Akubra-Internal-Host-3yeLjyjQNx"
	// InternalBucketHeader used for rewriting domain style
	InternalBucketHeader = "X-Akubra-Internal-Bucket-lejK0EpVZy"
	// PathStyleFormat is a S3 path style format
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
		return nil, fmt.Errorf("Missing host header, request id %d", req.Context().Value(log.ContextreqIDKey))
	}
	if bucket != "" {
		pathStyleURL := ""
		if len(req.URL.Path) > 0 {
			pathStyleURL = fmt.Sprintf("/%s%s", bucket, req.URL.Path)
		} else {
			pathStyleURL = fmt.Sprintf("/%s", bucket)
		}
		log.Debugf("Rewritten domain style url (%s) to path style url (%s) for request %s",
			req.Context().Value(log.ContextreqIDKey), req.URL.Path, pathStyleURL)
		req.URL.Path = pathStyleURL
	}
	req.Host = host
	req.URL.Host = host
	req.Header.Del(InternalHostHeader)
	req.Header.Del(InternalBucketHeader)
	return interceptor.roundTripper.RoundTrip(req)
}