package http

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type DiscoveryMock struct {
	*mock.Mock
}

func TestShouldUseDiscoveryClientAndMakeRequest(t *testing.T) {
	for _, address := range []string{"service://test-service", "http://127.0.0.1"} {
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			assert.True(t, http.MethodGet == req.Method)
			assert.Equal(t, req.URL.Path, "/unit/test")
			rw.WriteHeader(200)
			_, _ = rw.Write([]byte("passed"))
		}))

		serverURL, err := url.Parse(server.URL)
		assert.Nil(t, err)

		discoveryMock := DiscoveryMock{Mock: &mock.Mock{}}

		if strings.HasPrefix(address, "http") {
			address = fmt.Sprintf("%s:%s", address, serverURL.Port())
		} else {
			endpoint, err := url.Parse(fmt.Sprintf("http://127.0.0.1:%s", serverURL.Port()))
			assert.Nil(t, err)

			discoveryMock.On("GetEndpoint", "test-service").Return(endpoint, nil)
		}

		request, _ := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/unit/test", address), nil)

		cli := NewDiscoveryHTTPClient(&discoveryMock, http.DefaultClient)
		resp, err := cli.Do(request)
		assert.Nil(t, err)
		assert.Equal(t, resp.StatusCode, http.StatusOK)

		b, err := ioutil.ReadAll(resp.Body)
		assert.Nil(t, err)
		assert.Equal(t, b, []byte("passed"))
	}
}

func (mock *DiscoveryMock) GetEndpoint(serviceName string) (*url.URL, error) {
	args := mock.Called(serviceName)
	var u *url.URL
	if args.Get(0) != nil {
		u = args.Get(0).(*url.URL)
	}
	return u, args.Error(1)
}
