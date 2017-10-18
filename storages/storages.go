package storages

import (
	"fmt"
	"net/http"
	"net/url"

	config "github.com/allegro/akubra/storages/config"
)

// Cluster stores information about cluster backends
type Cluster struct {
	Backends []http.RoundTripper
	Name     string
}

//Storages config
type Storages struct {
	clustersConf config.ClustersMap
	backendsConf config.BackendsMap
	transport    http.RoundTripper
	Clusters     map[string]*Cluster
	Backends     map[string]*Backend
}

// Backend represents any storage in akubra cluster
type Backend struct {
	http.RoundTripper
	Endpoint url.URL
}

// RoundTrip satisfies http.RoundTripper interface
func (b *Backend) RoundTrip(r *http.Request) (*http.Response, error) {
	r.URL.Host = b.Endpoint.Host
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

// RoundTrip implements http.RoundTripper interface
func (c Cluster) RoundTrip(req *http.Request) (*http.Response, error) {
	// TODO: FIXME
	return c.RoundTrip(req)
}

func newBackend(backendConfig config.Backend, transport http.RoundTripper) (*Backend, error) {
	return &Backend{Endpoint: *backendConfig.Endpoint.URL, RoundTripper: transport}, nil
}

func newCluster(name string, clusterConf config.Cluster, backends map[string]*Backend) (*Cluster, error) {
	clusterBackends := make([]http.RoundTripper, 0)
	for _, backendName := range clusterConf.Backends {
		backendRT, ok := backends[backendName]
		if !ok {
			return nil, fmt.Errorf("No such backend %q", backendName)
		}

		clusterBackends = append(clusterBackends, backendRT)
	}

	return &Cluster{Backends: clusterBackends, Name: name}, nil
}

//GetCluster gets cluster by name or nil if cluster with given name was not found
func (st Storages) GetCluster(name string) (*Cluster, error) {
	s3cluster, ok := st.Clusters[name]
	if ok {
		return s3cluster, nil
	}
	return nil, fmt.Errorf("No such cluster defined %q", name)
}

// InitStorages setups storages
func InitStorages(transport http.RoundTripper, clustersConf config.ClustersMap, backendsConf config.BackendsMap) (*Storages, error) {
	clusters := make(map[string]*Cluster)
	backends := make(map[string]*Backend)
	for name, backendConf := range backendsConf {
		backends[name] = &Backend{
			transport,
			*backendConf.Endpoint.URL,
		}

	}
	for name, clusterConf := range clustersConf {
		cluster, err := newCluster(name, clusterConf, backends)
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
	}, nil
}
