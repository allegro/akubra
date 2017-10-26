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

// Cluster stores information about cluster backends
type Cluster struct {
	Backends    []http.RoundTripper
	Name        string
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
	Clusters     map[string]Cluster
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
		c.Backends,
		c.respHandler,
		nil)
	c.transport = multiTransport
}

// RoundTrip implements http.RoundTripper interface
func (c Cluster) RoundTrip(req *http.Request) (*http.Response, error) {
	return c.transport.RoundTrip(req)
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

	cluster := &Cluster{Backends: clusterBackends, Name: name, respHandler: respHandler}
	cluster.setupRoundTripper()
	return cluster, nil
}

//GetCluster gets cluster by name or nil if cluster with given name was not found
func (st Storages) GetCluster(name string) (Cluster, error) {
	s3cluster, ok := st.Clusters[name]
	if ok {
		return s3cluster, nil
	}
	return Cluster{}, fmt.Errorf("No such cluster defined %q", name)
}

// JoinClusters returns Cluster of joinded clusters backends
func (st *Storages) JoinClusters(name string, clusters ...Cluster) Cluster {
	cluster, ok := st.Clusters[name]
	if ok {
		return cluster
	}
	backends := make([]http.RoundTripper, 0)
	for _, cluster := range clusters {
		backends = append(backends, cluster.Backends...)
	}

	cluster = Cluster{Backends: backends, Name: name, respHandler: st.respHandler}
	cluster.setupRoundTripper()
	st.Clusters[name] = cluster
	return cluster
}

// InitStorages setups storages
func InitStorages(transport http.RoundTripper, clustersConf config.ClustersMap, backendsConf config.BackendsMap, respHandler transport.MultipleResponsesHandler) (*Storages, error) {
	clusters := make(map[string]Cluster)
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
		clusters[name] = *cluster
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
