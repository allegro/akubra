package discovery

import (
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"time"

	"net"

	"github.com/hashicorp/consul/api"
)

const (
	httpRequestTimeout       = time.Second * 3
	httpConsulRequestTimeout = time.Second * 2
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
