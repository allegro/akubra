package http

import (
	"net/http"

	"github.com/allegro/akubra/discovery"
)

var (
	serviceScheme = "service"
)

//DiscoveryHTTPClient is a dicovery based http client
type DiscoveryHTTPClient struct {
	discoveryClient discovery.Client
	httpClient      *http.Client
}

//NewDiscoveryHTTPClient creates an instance of DicoveryHTTPClient
func NewDiscoveryHTTPClient(discoveryClient discovery.Client, httpClient *http.Client) Client {
	return &DiscoveryHTTPClient{
		discoveryClient: discoveryClient,
		httpClient:      httpClient,
	}
}

//Do resolves the service address and makes the request
func (httpClient *DiscoveryHTTPClient) Do(request *http.Request) (*http.Response, error) {
	if request.URL.Scheme == serviceScheme {
		err := httpClient.resolveAddress(request)
		if err != nil {
			return nil, err
		}
	}
	return httpClient.httpClient.Do(request)
}

func (httpClient *DiscoveryHTTPClient) resolveAddress(request *http.Request) error {
	serviceName := request.URL.Hostname()
	serviceEndpoint, err := httpClient.discoveryClient.GetEndpoint(serviceName)
	if err != nil {
		return err
	}

	request.Host = serviceEndpoint.Host
	request.Header.Set("Host", serviceEndpoint.Host)
	request.URL.Host = serviceEndpoint.Host
	request.URL.Scheme = serviceEndpoint.Scheme

	return nil
}
