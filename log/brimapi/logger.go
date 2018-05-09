package brimapi

import (
	"bytes"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"

	"fmt"

	"github.com/allegro/akubra/service"
	"github.com/hashicorp/consul/api"
	"github.com/sirupsen/logrus"
)

// Credentials stores brim api credentials
type Credentials struct {
	User string `json:"User"`
	Pass string `json:"Pass"`
}

const (
	uploadSynctasksURI         = "/v1/processes/uploadsynctasks"
	serviceDiscoverySchemeName = "service"
)

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

// LogHook collects and sends sync events to brim api
type LogHook struct {
	Creds Credentials `json:"Credentials"`
	Host  string      `json:"Host"`
}

// Levels for logrus.Hook interface complience
func (lh *LogHook) Levels() []logrus.Level {
	return logrus.AllLevels
}

// Fire for logrus.Hook interface compliance
func (lh *LogHook) Fire(entry *logrus.Entry) error {
	bodyBytes := []byte(entry.Message)
	uri, err := lh.discoveryServiceURI(lh.Host)
	if err != nil {
		return err
	}
	uri.Path = uploadSynctasksURI
	req, err := http.NewRequest(
		http.MethodPut,
		uri.String(),
		bytes.NewBuffer(bodyBytes))
	if err != nil {
		return err
	}
	req.SetBasicAuth(lh.Creds.User, lh.Creds.Pass)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	return discardBody(resp)
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

	consulClient, err := api.NewClient(api.DefaultConfig())
	if err != nil {
		return nil, fmt.Errorf("unable to create Consul client: %s", err)
	}
	resolver := service.New(consulClient)
	return resolver.GetRandomNode(parsedURI.Host)
}
