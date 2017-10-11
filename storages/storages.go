package storages

import (
	"fmt"
	"net/http"
	"net/url"

	"github.com/allegro/akubra/config"
	"github.com/allegro/akubra/httphandler"
	storagesconfig "github.com/allegro/akubra/storages/config"
)

// Cluster stores information about cluster backends
type Cluster struct {
	Backends []http.RoundTripper
	Name     string
}

//Storages config
type Storages struct {
	Conf      config.Config
	Transport http.RoundTripper
	Clusters  map[string]*Cluster
}

// Backend represents any storage in akubra cluster
type Backend struct {
	http.RoundTripper
	Endpoint url.URL
	// typ
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

func newBackend(backendConfig storagesconfig.Backend, config config.Config) (*Backend, error) {
	roundTripper, err := httphandler.ConfigureHTTPTransport(config)
	if err != nil {
		return nil, err
	}
	return &Backend{Endpoint: *backendConfig.Endpoint.URL, RoundTripper: roundTripper}, nil
}

func newMultiBackendCluster(name string, clusterConf storagesconfig.Cluster, conf config.Config) (*Cluster, error) {
	clusterBackends := make([]http.RoundTripper, 0)
	for _, backendName := range clusterConf.Backends {
		backendRT, err := newBackend(conf.BackendsMap[backendName], conf)
		if err != nil {
			return nil, fmt.Errorf("backend %s is improperly configured, reason: %s", backendName, err)
		}

		clusterBackends = append(clusterBackends, backendRT)
	}

	return &Cluster{Backends: clusterBackends, Name: name}, nil
}

func (st Storages) initCluster(name string) (*Cluster, error) {
	clusterConf, ok := st.Conf.Clusters[name]
	if !ok {
		return nil, fmt.Errorf("no cluster %q in configuration", name)
	}
	return newMultiBackendCluster(name, clusterConf, st.Conf)
}

//GetCluster gets cluster by name or nil if cluster with given name was not found
func (st Storages) GetCluster(name string) (*Cluster, error) {
	s3cluster, ok := st.Clusters[name]
	if ok {
		return s3cluster, nil
	}
	s3cluster, err := st.initCluster(name)
	if err != nil {
		return s3cluster, err
	}
	st.Clusters[name] = s3cluster
	return s3cluster, nil
}
