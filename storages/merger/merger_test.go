package merger

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

type rtMock struct{ request *http.Request }

func (rt *rtMock) RoundTrip(req *http.Request) (*http.Response, error) {
	rt.request = req
	return nil, nil
}

func TestListV2Decorator(t *testing.T) {
	rt := &rtMock{}
	nextMarker := "marker"
	req, err := http.NewRequest("GET", "/bucket/", nil)
	q := req.URL.Query()
	q.Set("list-type", "2")
	q.Set("continuation-token", nextMarker)
	req.URL.RawQuery = q.Encode()
	assert.NoError(t, err)
	wrappedRoundTripper := ListV2Interceptor(rt)
	resp, err := wrappedRoundTripper.RoundTrip(req)
	assert.NoError(t, err)
	assert.Nil(t, resp)
	transfomedRequestQuery := rt.request.URL.Query()
	assert.Equal(t, nextMarker, transfomedRequestQuery.Get("start-after"))
}
