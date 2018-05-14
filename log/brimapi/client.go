package brimapi

import (
	"bytes"
	"fmt"
	"github.com/allegro/akubra/service"
	"github.com/hashicorp/consul/api"
	"github.com/sirupsen/logrus"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	uploadSynctasksURI         = "/v1/processes/uploadsynctasks"
	serviceDiscoverySchemeName = "service"
	httpRequestTimeout         = time.Second * 5
	warmUpTasksLimit           = 1000
)

type WarmUp struct {
	Payload string
	*LogHook
}

// TODO: DiscoveryServices -> discoveryServices
var (
	DiscoveryServices  *service.Services
	httpClient         *http.Client
	warmUpTasksCounter uint16
)

// TODO: WarmUpCache -> warmCache
var WarmUpCache = make(chan WarmUp, warmUpTasksLimit)

var countTTT = 0

func init() {
	var httpClient = &http.Client{
		Timeout: httpRequestTimeout,
	}

	consulClient, err := api.NewClient(&api.Config{
		Address: "consul-dev.qxlint:80",
		Scheme:  "http",
	})
	// TODO: rignt client below
	// consulClient, err := api.NewClient(api.DefaultConfig())
	if err != nil {
		fmt.Errorf("unable to create Consul client: %s", err)
	}

	//TODO: warmCache - why NOT ?????
	logrus.Infof("Starting warmup goroutine")
	go sendWarmUpSyncData(httpClient, WarmUpCache)

	DiscoveryServices = service.New(consulClient, logrus.Infof)
}

func sendWarmUpSyncData(httpClient *http.Client, warmup <-chan WarmUp) {
	for {
		select {
		case w := <-warmup:
			logrus.Infof("GOT FROM WARMUP CACHE msg: %s", w.Payload)
			resp, err := doRequestWithDiscoveryService(w.LogHook, httpClient, w.Payload)
			if err == nil {
				err = discardBody(resp)
				if err != nil {
					logrus.Infof("ADD WARMUP CACHE AGAIN - discardBody error")
					WarmUpCache <- w
					time.Sleep(1 * time.Second)
				}
			} else {
				logrus.Infof("ADD WARMUP CACHE AGAIN - request error")
				WarmUpCache <- w
				time.Sleep(2 * time.Second)
			}
		}
	}
}

/*

func PrepareWarmUpPayload(warmup <-chan WarmUp) {
	for {
		select {
		case w := <-warmup:
			logrus.Infof("GOT FROM WARMUP CACHE msg: %s", w.Payload)
			if countTTT > 5 {
				logrus.Infof("PUT AGAIN TO WARMUP CACHE msg: %s", w.Payload)
				WarmUpCache <- w
				time.Sleep(3 * time.Second)
			}
			time.Sleep(1 * time.Second)
			countTTT++
		}
	}
}
*/

func doRequest(lh *LogHook, httpClient *http.Client, payload string) error {
	resp, err := doRequestWithDiscoveryService(lh, httpClient, payload)
	if err != nil {
		return fmt.Errorf("problem with sync task with payload: %s - err: %s", payload, err)
	}
	return discardBody(resp)
}

func doRequestWithDiscoveryService(lh *LogHook, httpClient *http.Client, payload string) (resp *http.Response, err error) {
	uri, discoveryServiceErr := lh.discoveryServiceURI(lh.Host)
	logrus.Infof("    - uri: %s", uri)

	if discoveryServiceErr != nil {
		//TODO: revert below after warmup tests
		warmup := WarmUp{
			Payload: payload,
			LogHook: lh,
		}
		logrus.Infof("ADD PAYLOLAD TO WARMUP CACHE: %s", warmup.Payload)
		WarmUpCache <- warmup
		return nil, discoveryServiceErr
	}
	uri.Path = uploadSynctasksURI
	req, reqRrr := http.NewRequest(
		http.MethodPut,
		uri.String(),
		bytes.NewBuffer([]byte(payload)))
	if reqRrr != nil {
		return nil, reqRrr
	}
	req.SetBasicAuth(lh.Creds.User, lh.Creds.Pass)
	resp, err = httpClient.Do(req)
	return
}

// Discovery service URI for service:// protocol
func (lh *LogHook) discoveryServiceURI(URI string) (*url.URL, error) {
	parsedURI, err := url.Parse(URI)
	if err != nil {
		return nil, err
	}
	if !strings.HasPrefix(parsedURI.Scheme, serviceDiscoverySchemeName) {
		return parsedURI, nil
	}
	return DiscoveryServices.GetEndpoint(parsedURI.Host)
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
