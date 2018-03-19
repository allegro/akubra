package storages

import (
	"net/http"
	"fmt"
)

const (
	HOST              = "X-Host"
	BUCKET            = "X-Bucket"
	PATH_STYLE_FORMAT = "/%s%s"
)

type domainStyleRewriter struct {
	roundTripper http.RoundTripper
}

var domainStyleDecorator = func(roundTripper http.RoundTripper) http.RoundTripper {
	return &domainStyleRewriter{roundTripper}
}

func (rewriter *domainStyleRewriter) RoundTrip(req *http.Request) (*http.Response, error) {

	host := req.Header.Get(HOST)
	bucket := req.Header.Get(BUCKET)

	if host == "" {
		return nil, fmt.Errorf("Missing host header!")
	}

	if bucket != "" {
		req.URL.Path = fmt.Sprintf(PATH_STYLE_FORMAT, bucket, req.URL.Path)
		req.Header.Del(BUCKET)
	}

	req.URL.Host = host
	req.Header.Del(HOST)

	return rewriter.roundTripper.RoundTrip(req)
}
