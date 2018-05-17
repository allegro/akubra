package storages

import (
	"fmt"
	"net/http"

	"github.com/allegro/akubra/httphandler"
	"github.com/allegro/akubra/log"
	"github.com/allegro/akubra/transport"

	"github.com/allegro/akubra/storages/auth"
	"github.com/allegro/akubra/storages/config"
	"github.com/allegro/akubra/storages/merger"
)

// Storages config
type Storages struct {
	clustersConf     config.ClustersMap
	backendsConf     config.BackendsMap
	Clusters         map[string]NamedCluster
	Backends         map[string]http.RoundTripper
	lateRespHandler  transport.MultipleResponsesHandler
	earlyRespHandler transport.MultipleResponsesHandler
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
	rh := responseMerger{merger: st.lateRespHandler}
	newCluster := &Cluster{backends: backends, name: name, respHandler: rh.responseHandler}
	newCluster.setupRoundTripper(syncLog, false)
	st.Clusters[name] = newCluster
	return newCluster
}

// InitStorages setups storages
func InitStorages(transport http.RoundTripper, clustersConf config.ClustersMap, backendsConf config.BackendsMap, earlyRespHandler, lateRespHandler transport.MultipleResponsesHandler, syncLog log.Logger) (*Storages, error) {
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
		cluster, err := newCluster(name, clusterConf.Backends, backends, earlyRespHandler, syncLog)
		if err != nil {
			return nil, err
		}
		clusters[name] = cluster
	}
	return &Storages{
		clustersConf:     clustersConf,
		backendsConf:     backendsConf,
		Clusters:         clusters,
		Backends:         backends,
		earlyRespHandler: earlyRespHandler,
		lateRespHandler:  lateRespHandler,
	}, nil
}

func decorateBackend(transport http.RoundTripper, name string, backendConf config.Backend) (http.RoundTripper, error) {

	errPrefix := fmt.Sprintf("initialization of backend '%s' resulted with error", name)
	decoratorFactory, ok := auth.Decorators[backendConf.Type]
	if !ok {
		return nil, fmt.Errorf("%s: no decorator defined for type '%s'", errPrefix, backendConf.Type)
	}
	decorator, err := decoratorFactory(name, backendConf)
	if err != nil {
		return nil, fmt.Errorf("%s: %q", errPrefix, err)
	}
	backend := &Backend{
		httphandler.Decorate(transport, decorator, merger.ListV2Interceptor),
		*backendConf.Endpoint.URL,
		name,
		backendConf.Maintenance,
	}
	return backend, nil
}
