package storages

import (
	"fmt"
	"net/http"
	"net/url"

	"github.com/allegro/akubra/httphandler"
	"github.com/allegro/akubra/log"
	"github.com/allegro/akubra/transport"

	"github.com/allegro/akubra/storages/auth"
	config "github.com/allegro/akubra/storages/config"
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

//Storages config
type Storages struct {
	clustersConf config.ClustersMap
	backendsConf config.BackendsMap
	transport    http.RoundTripper
	Clusters     map[string]NamedCluster
	Backends     map[string]http.RoundTripper
	respHandler  transport.MultipleResponsesHandler
}

// Backend represents any storage in akubra cluster
type Backend struct {
	http.RoundTripper
	Endpoint url.URL
	Name     string
}

// RoundTrip satisfies http.RoundTripper interface
func (b *Backend) RoundTrip(r *http.Request) (*http.Response, error) {
	r.URL.Host = b.Endpoint.Host
	r.URL.Scheme = "http"
	reqID := r.Context().Value(log.ContextreqIDKey)
	log.Debugf("Request %s req.URL.Host replaced with %s", reqID, r.URL.Host)
	return b.RoundTripper.RoundTrip(r)
}

func (c *Cluster) setupRoundTripper() {
	multiTransport := transport.NewMultiTransport(
		c.Backends(),
		c.respHandler,
		nil)
	c.transport = multiTransport
}

// RoundTrip implements http.RoundTripper interface
func (c *Cluster) RoundTrip(req *http.Request) (*http.Response, error) {
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

func newBackend(backendConfig config.Backend, transport http.RoundTripper) (*Backend, error) {
	return &Backend{Endpoint: *backendConfig.Endpoint.URL, RoundTripper: transport}, nil
}

func newCluster(name string, backendNames []string, backends map[string]http.RoundTripper, respHandler transport.MultipleResponsesHandler) (*Cluster, error) {
	clusterBackends := make([]http.RoundTripper, 0)
	for _, backendName := range backendNames {
		backendRT, ok := backends[backendName]
		if !ok {
			return nil, fmt.Errorf("No such backend %q", backendName)
		}

		clusterBackends = append(clusterBackends, backendRT)
	}

	cluster := &Cluster{backends: clusterBackends, name: name, respHandler: respHandler}
	cluster.setupRoundTripper()
	return cluster, nil
}

//GetCluster gets cluster by name or nil if cluster with given name was not found
func (st Storages) GetCluster(name string) (NamedCluster, error) {
	s3cluster, ok := st.Clusters[name]
	if ok {
		return s3cluster, nil
	}
	return &Cluster{}, fmt.Errorf("No such cluster defined %q", name)
}

// JoinClusters extends Clusters list of Storages by cluster made of joined clusters backends and returns it.
// If cluster of given name is already defined returns previously defined cluster instead.
func (st *Storages) JoinClusters(name string, clusters ...NamedCluster) NamedCluster {
	cluster, ok := st.Clusters[name]
	if ok {
		return cluster
	}
	backends := make([]http.RoundTripper, 0)
	for _, cluster := range clusters {
		backends = append(backends, cluster.Backends()...)
	}

	newCluster := &Cluster{backends: backends, name: name, respHandler: st.respHandler}
	newCluster.setupRoundTripper()
	st.Clusters[name] = newCluster
	return newCluster
}

// InitStorages setups storages
func InitStorages(transport http.RoundTripper, clustersConf config.ClustersMap, backendsConf config.BackendsMap, respHandler transport.MultipleResponsesHandler) (*Storages, error) {
	clusters := make(map[string]NamedCluster)
	backends := make(map[string]http.RoundTripper)
	for name, backendConf := range backendsConf {
		backend := &Backend{
			transport,
			*backendConf.Endpoint.URL,
			name,
		}
		decoratorFactory, ok := auth.Decorators[backendConf.Type]
		if !ok {
			return nil, fmt.Errorf("initialization of backend %s has resulted with error: no decorator defined for type %s", name, backendConf.Type)
		}
		decorator, err := decoratorFactory(backendConf.Extra)
		if err != nil {
			return nil, fmt.Errorf("initialization of backend %s has resulted with error: %q", name, err)
		}
		decoratedBackend := httphandler.Decorate(backend, decorator)
		backends[name] = decoratedBackend

	}
	for name, clusterConf := range clustersConf {
		cluster, err := newCluster(name, clusterConf.Backends, backends, respHandler)
		if err != nil {
			return nil, err
		}
		clusters[name] = cluster
	}
	return &Storages{
		clustersConf: clustersConf,
		backendsConf: backendsConf,
		transport:    transport,
		Clusters:     clusters,
		Backends:     backends,
		respHandler:  respHandler,
	}, nil
}
