package storages

import (
	"fmt"
	"net/http"

	set "github.com/deckarep/golang-set"
)

// NamedCluster interface
type NamedCluster interface {
	http.RoundTripper
	Name() string
	Backends() []*Backend
}

// Cluster stores information about cluster backends
type Cluster struct {
	backends          []*Backend
	name              string
	synclog           *SyncSender
	MethodSet         set.Set
	requestDispatcher dispatcher
}

// RoundTrip implements http.RoundTripper interface
func (c *Cluster) RoundTrip(req *http.Request) (*http.Response, error) {
	return c.requestDispatcher.Dispatch(req)
}

// Name get Cluster name
func (c *Cluster) Name() string {
	return c.name
}

// Backends get http.RoundTripper slice
func (c *Cluster) Backends() []*Backend {
	return c.backends
}

func newCluster(name string, backendNames []string, backends map[string]*Backend, synclog *SyncSender) (*Cluster, error) {
	clusterBackends := make([]*Backend, 0)
	for _, backendName := range backendNames {
		backendRT, ok := backends[backendName]
		if !ok {
			return nil, fmt.Errorf("no such backend %q in 'storages::newCluster'", backendName)
		}
		clusterBackends = append(clusterBackends, backendRT)
	}

	cluster := &Cluster{backends: clusterBackends, name: name, requestDispatcher: NewRequestDispatcher(clusterBackends, synclog), synclog: synclog}
	return cluster, nil
}
