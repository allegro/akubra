package storages

import (
	"fmt"
	"net/http"

	"github.com/allegro/akubra/log"
	"github.com/allegro/akubra/transport"
	set "github.com/deckarep/golang-set"
)

// NamedCluster interface
type NamedCluster interface {
	http.RoundTripper
	Name() string
	Backends() []http.RoundTripper
}

// Cluster stores information about cluster backends
type Cluster struct {
	backends    []http.RoundTripper
	name        string
	Logger      log.Logger
	MethodSet   set.Set
	respHandler transport.MultipleResponsesHandler
	transport   http.RoundTripper
}

func (c *Cluster) setupRoundTripper(syncLog log.Logger, enableMultipart bool) {
	log.Debugf("Cluster %s enabled mp %t", c.Name(), enableMultipart)
	multiTransport := transport.NewMultiTransport(
		c.Backends(),
		c.respHandler)

	c.transport = multiTransport
	if enableMultipart {
		clusterRoundTripper := NewMultiPartRoundTripper(c, syncLog)

		c.transport = clusterRoundTripper
		log.Debugf("Cluster %s has multipart setup successfully", c.name)
	}
}

// RoundTrip implements http.RoundTripper interface
func (c *Cluster) RoundTrip(req *http.Request) (*http.Response, error) {
	log.Debugf("RT cluster %s, %T", c.Name(), c.transport)
	return c.transport.RoundTrip(req)
}

// Name get Cluster name
func (c *Cluster) Name() string {
	return c.name
}

// Backends get http.RoundTripper slice
func (c *Cluster) Backends() []http.RoundTripper {
	return c.backends
}

func newCluster(name string, backendNames []string, backends map[string]http.RoundTripper, respHandler transport.MultipleResponsesHandler, synclog log.Logger) (*Cluster, error) {
	clusterBackends := make([]http.RoundTripper, 0)
	if len(backendNames) == 0 {
		return nil, fmt.Errorf("empty 'backendNames' map in 'storages::newCluster'")
	}
	if len(backends) == 0 {
		return nil, fmt.Errorf("empty 'backends' map in 'storages::newCluster'")
	}
	for _, backendName := range backendNames {
		backendRT, ok := backends[backendName]

		if !ok {
			return nil, fmt.Errorf("no such backend %q in 'storages::newCluster'", backendName)
		}
		clusterBackends = append(clusterBackends, backendRT)
	}

	cluster := &Cluster{backends: clusterBackends, name: name, respHandler: respHandler}
	cluster.setupRoundTripper(synclog, true)
	return cluster, nil
}
