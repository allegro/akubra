package storages

import (
	"net/http"
	"fmt"
	"github.com/allegro/akubra/regions"
)
const PATH_STYLE_FORMAT = "/%s%s"

type domainStyleRewriter struct {
	roundTripper http.RoundTripper
}

var domainStyleDecorator = func(roundTripper http.RoundTripper) http.RoundTripper {
	return &domainStyleRewriter{roundTripper}
}

func (rewriter *domainStyleRewriter) RoundTrip(req *http.Request) (*http.Response, error)  {

	host := req.Header.Get(regions.HOST)
	bucket := req.Header.Get(regions.BUCKET)

	if host == ""  {
		return nil, fmt.Errorf("Missing host header!")
	}

	if bucket != "" {
		req.URL.Path = fmt.Sprintf(PATH_STYLE_FORMAT, bucket, req.URL.Path)
		req.Header.Del(regions.BUCKET)
	}


	req.URL.Host = host
	req.Header.Del(regions.HOST)

	return rewriter.roundTripper.RoundTrip(req)
}