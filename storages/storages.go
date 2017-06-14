package storages

import (
	"fmt"
	"net/http"
	"net/url"

	"github.com/allegro/akubra/config"
	"github.com/allegro/akubra/httphandler"
	shardingconfig "github.com/allegro/akubra/sharding/config"
	"github.com/allegro/akubra/transport"
)

// Cluster stores information about cluster backends
type Cluster struct {
	http.RoundTripper
	Backends []shardingconfig.YAMLUrl
	Name     string
}

//Storages config
type Storages struct {
	Conf      config.Config
	Transport http.RoundTripper
	Clusters  map[string]Cluster
}

func newMultiBackendCluster(transp http.RoundTripper,
	multiResponseHandler transport.MultipleResponsesHandler,
	clusterConf shardingconfig.ClusterConfig, name string, maintainedBackends []shardingconfig.YAMLUrl) Cluster {
	backends := make([]url.URL, len(clusterConf.Backends))

	for i, backend := range clusterConf.Backends {
		backends[i] = *backend.URL
	}

	multiTransport := transport.NewMultiTransport(
		transp,
		backends,
		multiResponseHandler,
		maintainedBackends)

	return Cluster{
		multiTransport,
		clusterConf.Backends,
		name,
	}
}

func (st Storages) initCluster(name string) (Cluster, error) {
	clusterConf, ok := st.Conf.Clusters[name]
	if !ok {
		return Cluster{}, fmt.Errorf("no cluster %q in configuration", name)
	}
	respHandler := httphandler.EarliestResponseHandler(st.Conf)
	return newMultiBackendCluster(st.Transport, respHandler, clusterConf, name, st.Conf.MaintainedBackends), nil
}

//GetCluster gets cluster by name or nil if cluster with given name was not found
func (st Storages) GetCluster(name string) (Cluster, error) {
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
