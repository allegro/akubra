package storages

import (
	"fmt"
	"net/http"
	"net/url"

	"github.com/allegro/akubra/transport"

	"github.com/allegro/akubra/log"

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
	Backends     map[string]Backend
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
	log.Printf("Bylem tu %s", r.URL)
	r.URL.Host = b.Endpoint.Host
	log.Printf("Bylem tu 2 %s", r.URL)
	return b.RoundTripper.RoundTrip(r)
}

// func newMultiBackendCluster(
// 	transp http.RoundTripper,
// 	multiResponseHandler transport.MultipleResponsesHandler,
// 	clusterConf storagesconfig.Cluster,
// 	name string,
// 	maintainedBackends []shardingconfig.YAMLUrl,
// 	backendsMap storagesconfig.BackendsMap) Cluster {

// 	backends := make([]http.RoundTripper, len(clusterConf.Backends))

// 	for i, backend := range clusterConf.Backends {
// 		backends[i] = &Backend{transp, backendsMap[backend].Endpoint}
// 	}

// 	multiTransport := transport.NewMultiTransport(
// 		backends,
// 		multiResponseHandler,
// 		maintainedBackends)

// 	return Cluster{
// 		multiTransport,
// 		clusterConf.Backends,
// 		name,
// 	}
// }

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

func newCluster(name string, backendNames []string, backends map[string]Backend, respHandler transport.MultipleResponsesHandler) (*Cluster, error) {
	clusterBackends := make([]http.RoundTripper, 0)
	for _, backendName := range backendNames {
		backendRT, ok := backends[backendName]
		if !ok {
			return nil, fmt.Errorf("No such backend %q", backendName)
		}

		clusterBackends = append(clusterBackends, &backendRT)
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
	backends := make(map[string]Backend)
	for name, backendConf := range backendsConf {
		backends[name] = Backend{
			transport,
			*backendConf.Endpoint.URL,
			name,
		}

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
