package storages

import (
	"net/http"
	"fmt"
	"github.com/allegro/akubra/log"
)

const (
	// InternalHostHeader is used for rewriting domain style
	InternalHostHeader   = "X-Akubra-Internal-Host-3yeLjyjQNx"
	// InternalBucketHeader used for rewriting domain style
	InternalBucketHeader = "X-Akubra-Internal-Bucket-lejK0EpVZy"
	// PathStyleFormat is a S3 path style format
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
		return nil, fmt.Errorf("Missing host header, request id %d", req.Context().Value(log.ContextreqIDKey))
	}
	if bucket != "" {
		oldPath := ""
		if len(req.URL.Path) > 0 {
			oldPath = req.URL.Path[1:]
		}
		pathStyleUrl := fmt.Sprintf(PathStyleFormat, bucket, oldPath)
		log.Debugf("Rewritten domain style url (%s) to path style url (%s) for request %s",
			req.Context().Value(log.ContextreqIDKey), req.URL.Path, pathStyleUrl)
		req.URL.Path = pathStyleUrl
	}
	req.Host = host
	req.URL.Host = host
	req.Header.Del(InternalHostHeader)
	req.Header.Del(InternalBucketHeader)
	return interceptor.roundTripper.RoundTrip(req)
}