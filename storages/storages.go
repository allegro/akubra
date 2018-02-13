package storages

import (
	"fmt"
	"net/http"
	"net/url"

	"github.com/allegro/akubra/log"
	"github.com/allegro/akubra/transport"

	"github.com/allegro/akubra/storages/auth"
	"github.com/allegro/akubra/storages/config"
	set "github.com/deckarep/golang-set"
	"github.com/allegro/akubra/httphandler"
)

type backendError struct {
	backend string
	origErr error
}

func (be *backendError) Backend() string {
	return be.backend
}

func (be *backendError) Err() error {
	return be.origErr
}

func (be *backendError) Error() string {
	return fmt.Sprintf("backend %s responded with error %s", be.backend, be.origErr)
}

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
		log.Debugf("Request %s blocked %s is in maintenance mode", reqID, r.URL.Host)
		return nil, &backendError{backend: b.Endpoint.Host,
			origErr: fmt.Errorf("backend %v in maintenance mode", b.Name)}
	}
	err := error(nil)
	resp, oerror := b.RoundTripper.RoundTrip(r)

	if oerror != nil {
		err = &backendError{backend: b.Endpoint.Host, origErr: oerror}
	}
	return resp, err
}

func (c *Cluster) setupRoundTripper(syncLog log.Logger) {

	multiPartUploadBackend, backendsHostnamesToSync := PickRandomBackendForMultiPartUpload(c.Backends())
	multiPartUploadHandler := httphandler.NewMultiPartUploadHandler(multiPartUploadBackend, c.transport, syncLog, backendsHostnamesToSync)

	multiTransport := transport.NewMultiTransport(
		c.Backends(),
		c.respHandler)

	if multiPartUploadBackend != nil {

		c.transport = httphandler.Decorate(
			multiTransport,
			httphandler.MultiPartUploadDecorator(multiPartUploadHandler))

	} else {

		c.transport = multiTransport
	}
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
	cluster.setupRoundTripper(synclog)
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
func (st *Storages) ClusterShards(name string, syncLog log.Logger, clusters ...NamedCluster) NamedCluster {
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
	newCluster.setupRoundTripper(syncLog)
	st.Clusters[name] = newCluster
	return newCluster
}

// InitStorages setups storages
func InitStorages(transport http.RoundTripper, clustersConf config.ClustersMap, backendsConf config.BackendsMap,
				  respHandler transport.MultipleResponsesHandler, syncLog log.Logger) (*Storages, error) {

	clusters := make(map[string]NamedCluster)
	backends := make(map[string]http.RoundTripper)
	if len(backendsConf) == 0 {
		return nil, fmt.Errorf("empty map 'backendsConf' in 'InitStorages'")
	}
	for name, backendConf := range backendsConf {
		if backendConf.Maintenance {
			log.Printf("backend %q in maintenance mode", name)
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
		cluster, err := newCluster(name, clusterConf.Backends, backends, respHandler, syncLog)
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
	errPrefix := fmt.Sprintf("initialization of backend '%s' resulted with error", name)
	decoratorFactory, ok := auth.Decorators[backendConf.Type]
	if !ok {
		return nil, fmt.Errorf("%s: no decorator defined for type '%s'", errPrefix, backendConf.Type)
	}
	decorator, err := decoratorFactory(backendConf.Properties, name)
	if err != nil {
		return nil, fmt.Errorf("%s: %q", errPrefix, err)
	}
	return httphandler.Decorate(backend, decorator), nil
}
