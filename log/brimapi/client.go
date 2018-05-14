package brimapi

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/allegro/akubra/service"
	"github.com/hashicorp/consul/api"
	"github.com/sirupsen/logrus"
)

const (
	uploadSynctasksURI         = "/v1/processes/uploadsynctasks"
	serviceDiscoverySchemeName = "service"
	httpRequestTimeout         = time.Second * 3
	httpConsulRequestTimeout   = time.Second * 2
)

var (
	discoveryServices *service.Services
	httpClient        *http.Client
)

func init() {
	httpClient = &http.Client{
		Timeout: httpRequestTimeout,
	}
	consulConfig := api.DefaultConfig()
	api.DefaultConfig().Transport = &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   httpConsulRequestTimeout,
			DualStack: true,
		}).DialContext,
	}
	consulClient, err := api.NewClient(consulConfig)
	if err != nil {
		fmt.Printf("unable to create Consul client: %s", err)
	}

	discoveryServices = service.New(consulClient, service.DefaultCacheInvalidationTimeout)
}

func doRequest(lh *LogHook, httpClient *http.Client, payload string) (endpoint string, err error) {
	resp, endpoint, err := doRequestWithDiscoveryService(lh, httpClient, payload)
	if err != nil {
		return
	}
	return endpoint, discardBody(resp)
}

func doRequestWithDiscoveryService(lh *LogHook, httpClient *http.Client, payload string) (resp *http.Response, endpoint string, err error) {
	uri, discoveryServiceErr := lh.getServiceURIFromDiscoveryService(lh.Host)
	if discoveryServiceErr != nil {
		return nil, "", discoveryServiceErr
	}
	uri.Path = uploadSynctasksURI
	endpoint = uri.String()
	logrus.Debugf("sync task endpoint: %s", endpoint)

	req, requestErr := http.NewRequest(
		http.MethodPut,
		endpoint,
		bytes.NewBuffer([]byte(payload)))
	if requestErr != nil {
		return nil, endpoint, requestErr
	}
	req.SetBasicAuth(lh.Creds.User, lh.Creds.Pass)
	resp, err = httpClient.Do(req)
	return
}

func (lh *LogHook) getServiceURIFromDiscoveryService(URI string) (*url.URL, error) {
	parsedURI, err := url.Parse(URI)
	if err != nil {
		return nil, err
	}
	if !strings.HasPrefix(parsedURI.Scheme, serviceDiscoverySchemeName) {
		return parsedURI, nil
	}
	return discoveryServices.GetEndpoint(parsedURI.Host)
}

func discardBody(resp *http.Response) error {
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
