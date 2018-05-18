package discovery

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"time"

	"net"

	"github.com/hashicorp/consul/api"
)

const (
	serviceDiscoverySchemeName = "service"
	httpRequestTimeout         = time.Second * 3
	httpConsulRequestTimeout   = time.Second * 2
)

var (
	discoveryServices *Services
	httpClient        *http.Client
)

func init() {
	httpClient = &http.Client{
		Timeout: httpRequestTimeout,
	}
	consulConfig := api.DefaultConfig()
	consulConfig.Transport = &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   httpConsulRequestTimeout,
			DualStack: true,
		}).DialContext,
	}
	consulClient, err := NewClientWrapper(consulConfig, httpClient)
	if err != nil {
		panic(err)
	}

	discoveryServices = NewServices(consulClient, DefaultCacheInvalidationTimeout)
}

// NewClientWrapper returns a new Consul client wrapper
func NewClientWrapper(config *api.Config, httpClient *http.Client) (IClient, error) {
	var err error
	config.HttpClient = httpClient
	cfg := &ClientWrapper{}
	cfg.Client, err = api.NewClient(config)
	if err != nil {
		return nil, fmt.Errorf("unable to create new Consul client: %s", err)
	}
	return cfg, nil
}

// GetHTTPClient for service discovery
func GetHTTPClient() *http.Client {
	return httpClient
}

// DoRequestWithDiscoveryService with hostname (username + password) and payload
func DoRequestWithDiscoveryService(httpClient *http.Client, host, path, username, password, payload string) (resp *http.Response, endpoint string, err error) {
	serviceInstanceAddress, discoveryServiceErr := getServiceURIFromDiscoveryService(host)
	if discoveryServiceErr != nil {
		return nil, "", discoveryServiceErr
	}
	serviceInstanceAddress.Path = path
	endpoint = serviceInstanceAddress.String()

	req, requestErr := http.NewRequest(
		http.MethodPut,
		endpoint,
		bytes.NewBuffer([]byte(payload)))
	if requestErr != nil {
		return nil, endpoint, requestErr
	}
	req.SetBasicAuth(username, password)
	resp, err = httpClient.Do(req)
	return
}

// DiscardBody from response
func DiscardBody(resp *http.Response) error {
	if resp != nil && resp.Body != nil {
		_, err := io.Copy(ioutil.Discard, resp.Body)
		if err != nil {
			return err
		}
		err = resp.Body.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

func getServiceURIFromDiscoveryService(URI string) (*url.URL, error) {
	parsedURI, err := url.Parse(URI)
	if err != nil {
		return nil, err
	}
	if !strings.HasPrefix(parsedURI.Scheme, serviceDiscoverySchemeName) {
		return parsedURI, nil
	}
	url, err := discoveryServices.GetEndpoint(parsedURI.Host)
	return url, err
}
