package http

import (
	"errors"
	"net/http"

	"github.com/allegro/akubra/internal/akubra/discovery"
)

var (
	serviceScheme       = "service"
	ErrNotServiceScheme = errors.New("not a service scheme")
)

//DiscoveryHTTPClient is a dicovery based http client
type DiscoveryHTTPClient struct {
	discoveryClient *discovery.Services
	httpClient      *http.Client
}

//NewDiscoveryHTTPClient creates an instance of DicoveryHTTPClient
func NewDiscoveryHTTPClient(discoveryClient *discovery.Services, httpClient *http.Client) Client {
	return &DiscoveryHTTPClient{
		discoveryClient: discoveryClient,
		httpClient:      httpClient,
	}
}

//Do resolves the service address and makes the request
func (httpClient *DiscoveryHTTPClient) Do(request *http.Request) (*http.Response, error) {
	if request.URL.Scheme != serviceScheme {
		return nil, ErrNotServiceScheme
	}

	serviceName := request.URL.Hostname()
	serviceEndpoint, err := httpClient.discoveryClient.GetEndpoint(serviceName)
	if err != nil {
		return nil, err
	}

	request.Host = serviceEndpoint.Host
	request.Header.Set("Host", serviceEndpoint.Host)
	request.URL.Host = serviceEndpoint.Host

	return httpClient.httpClient.Do(request)
}
