package storages

import (
	"fmt"
	"net/http"

	"github.com/allegro/akubra/httphandler"
	"github.com/allegro/akubra/log"

	"github.com/allegro/akubra/storages/auth"
	"github.com/allegro/akubra/storages/config"
	"github.com/allegro/akubra/storages/merger"
)

// ClusterStorage is basic cluster storage interface
type ClusterStorage interface {
	GetCluster(name string) (NamedCluster, error)
	ClusterShards(name string, clusters ...NamedCluster) NamedCluster
}

// Storages config
type Storages struct {
	clustersConf config.ClustersMap
	backendsConf config.BackendsMap
	syncLog      *SyncSender
	Clusters     map[string]NamedCluster
	Backends     map[string]*Backend
}

// GetCluster gets cluster by name or nil if cluster with given name was not found
func (st *Storages) GetCluster(name string) (NamedCluster, error) {
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
	backendsNames := make([]string, 0)
	for _, cluster := range clusters {
		for _, backend := range cluster.Backends() {
			backendsNames = append(backendsNames, backend.Name)
		}
	}
	sCluster, err := newCluster(name, backendsNames, st.Backends, st.syncLog)
	if err != nil {
		log.Fatalf("Initialization of region cluster %s failed reason: %s", name, err)
	}
	st.Clusters[name] = sCluster
	return sCluster
}

// InitStorages setups storages
func InitStorages(transport http.RoundTripper, clustersConf config.ClustersMap, backendsConf config.BackendsMap, syncLog *SyncSender) (*Storages, error) {
	clusters := make(map[string]NamedCluster)
	backends := make(map[string]*Backend)
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
		cluster, err := newCluster(name, clusterConf.Backends, backends, syncLog)
		if err != nil {
			return nil, err
		}
		clusters[name] = cluster
	}

	return &Storages{
		clustersConf: clustersConf,
		backendsConf: backendsConf,
		syncLog:      syncLog,
		Clusters:     clusters,
		Backends:     backends,
	}, nil
}

func decorateBackend(transport http.RoundTripper, name string, backendConf config.Backend) (*Backend, error) {

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
		RoundTripper: httphandler.Decorate(transport, decorator, merger.ListV2Interceptor),
		Endpoint:     *backendConf.Endpoint.URL,
		Name:         name,
		Maintenance:  backendConf.Maintenance,
	}
	return backend, nil
}
