package http

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type DiscoveryMock struct {
	*mock.Mock
}

func TestRejectingRequestWithoutServiceScheme(t *testing.T) {
	nonServiceRequest, _ := http.NewRequest(http.MethodGet, "http://127.0.0.1", nil)
	cli := NewDiscoveryHTTPClient(nil, nil)
	resp, err := cli.Do(nonServiceRequest)
	assert.Nil(t, resp)
	assert.Equal(t, ErrNotServiceScheme, err)
}
func TestShouldUseDiscoveryClientAndMakeRequest(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		assert.True(t, http.MethodGet == req.Method)
		assert.Equal(t, req.URL.Path, "/unit/test")
		rw.WriteHeader(200)
		_, _ = rw.Write([]byte("passed"))
	}))

	serverURL, err := url.Parse(server.URL)
	assert.Nil(t, err)

	discoveryMock := DiscoveryMock{Mock: &mock.Mock{}}
	discoveryMock.On("GetEndpoint", "test-service").
		Return(fmt.Sprintf("127.0.0.1:%d", serverURL.Port()), nil)

	request, _ := http.NewRequest(http.MethodGet, "service://test-service/unit/test", nil)

	cli := NewDiscoveryHTTPClient(&discoveryMock, http.DefaultClient)
	resp, err := cli.Do(request)
	assert.Nil(t, err)
	assert.Equal(t, resp.StatusCode, http.StatusOK)

	b, err := ioutil.ReadAll(resp.Body)
	assert.Nil(t, err)
	assert.Equal(t, b, []byte("passed"))
}

func (mock *DiscoveryMock) GetEndpoint(serviceName string) (*url.URL, error) {
	args := mock.Called(serviceName)
	var u *url.URL
	if args.Get(0) != nil {
		u := args.Get(0).(*url.URL)
	}
	return u, args.Error(1)
}
