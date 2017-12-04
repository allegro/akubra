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

// Storages config
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
	Endpoint    url.URL
	Name        string
	Maintenance bool
}

// RoundTrip satisfies http.RoundTripper interface
func (b *Backend) RoundTrip(r *http.Request) (*http.Response, error) {
	r.URL.Host = b.Endpoint.Host
	r.URL.Scheme = b.Endpoint.Scheme
	reqID := r.Context().Value(log.ContextreqIDKey)
	log.Debugf("Request %s req.URL.Host replaced with %s", reqID, r.URL.Host)

	if b.Maintenance {
		return nil, fmt.Errorf("backend %v in maintenance mode", b.Name)
	}
	return b.RoundTripper.RoundTrip(r)
}

func (c *Cluster) setupRoundTripper() {
	multiTransport := transport.NewMultiTransport(
		c.Backends(),
		c.respHandler)
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
	if len(backendNames) == 0 {
		return nil, fmt.Errorf("empty 'backendNames' map in 'storages::newCluster'")
	}
	if len(backends) == 0 {
		return nil, fmt.Errorf("empty 'backends' map in 'storages::newCluster'")
	}
	for _, backendName := range backendNames {
		backendRT, ok := backends[backendName]
		if ok {
			clusterBackends = append(clusterBackends, backendRT)
		}
	}

	cluster := &Cluster{backends: clusterBackends, name: name, respHandler: respHandler}
	cluster.setupRoundTripper()
	return cluster, nil
}

// GetCluster gets cluster by name or nil if cluster with given name was not found
func (st Storages) GetCluster(name string) (NamedCluster, error) {
	s3cluster, ok := st.Clusters[name]
	if ok {
		return s3cluster, nil
	}
	return &Cluster{}, fmt.Errorf("no such cluster defined %q", name)
}

// ClusterShards extends Clusters list of Storages by cluster made of joined clusters backends and returns it.
// If cluster of given name is already defined returns previously defined cluster instead.
func (st *Storages) ClusterShards(name string, clusters ...NamedCluster) NamedCluster {
	cluster, ok := st.Clusters[name]
	if ok {
		return cluster
	}
	backends := make([]http.RoundTripper, 0)
	for _, cluster := range clusters {
		backends = append(backends, cluster.Backends()...)
	}
	rh := responseMerger{merger: st.respHandler}
	newCluster := &Cluster{backends: backends, name: name, respHandler: rh.responseHandler}
	newCluster.setupRoundTripper()
	st.Clusters[name] = newCluster
	return newCluster
}

// InitStorages setups storages
func InitStorages(transport http.RoundTripper, clustersConf config.ClustersMap, backendsConf config.BackendsMap, respHandler transport.MultipleResponsesHandler) (*Storages, error) {
	clusters := make(map[string]NamedCluster)
	backends := make(map[string]http.RoundTripper)
	if len(backendsConf) == 0 {
		return nil, fmt.Errorf("empty map 'backendsConf' in 'InitStorages'")
	}
	for name, backendConf := range backendsConf {
		if backendConf.Maintenance {
			log.Printf("backend %q in maintenance mode", name)
			continue
		}
		decoratedBackend, err := decorateBackend(transport, name, backendConf)
		if err != nil {
			return nil, err
		}
		backends[name] = decoratedBackend
	}
	if len(clustersConf) == 0 {
		return nil, fmt.Errorf("empty map 'clustersConf' in 'InitStorages'")
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

func decorateBackend(transport http.RoundTripper, name string, backendConf config.Backend) (http.RoundTripper, error) {
	backend := &Backend{
		transport,
		*backendConf.Endpoint.URL,
		name,
		backendConf.Maintenance,
	}
	decoratorFactory, ok := auth.Decorators[backendConf.Type]
	if !ok {
		return nil, fmt.Errorf("initialization of backend %s has resulted with error: no decorator defined for type %s", name, backendConf.Type)
	}
	decorator, err := decoratorFactory(backendConf.Properties, name)
	if err != nil {
		return nil, fmt.Errorf("initialization of backend %s has resulted with error: %q", name, err)
	}
	return httphandler.Decorate(backend, decorator), nil
}