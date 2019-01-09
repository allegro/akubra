package storages

import (
	"fmt"
	"net/http"

	"github.com/allegro/akubra/balancing"

	"github.com/allegro/akubra/httphandler"
	"github.com/allegro/akubra/log"

	"github.com/allegro/akubra/storages/auth"
	"github.com/allegro/akubra/storages/config"
	"github.com/allegro/akubra/storages/merger"
)

// ClusterStorage is basic cluster storage interface
type ClusterStorage interface {
	GetShard(name string) (NamedShardClient, error)
	MergeShards(name string, clusters ...NamedShardClient) NamedShardClient
}

// Storages config
type Storages struct {
	clustersConf config.ShardsMap
	storagesMap  config.StoragesMap
	syncLog      *SyncSender
	ShardClients map[string]NamedShardClient
	Backends     map[string]*StorageClient
}

// GetShard gets cluster by name or nil if cluster with given name was not found
func (st *Storages) GetShard(name string) (NamedShardClient, error) {
	s3cluster, ok := st.ShardClients[name]

	if ok {
		return s3cluster, nil
	}
	return &ShardClient{}, fmt.Errorf("no such shard defined %q", name)
}

// MergeShards extends Clusters list of Storages by cluster made of joined clusters backends and returns it.
// If cluster of given name is already defined returns previously defined cluster instead.
func (st *Storages) MergeShards(name string, clusters ...NamedShardClient) NamedShardClient {
	cluster, ok := st.ShardClients[name]
	if ok {
		return cluster
	}
	backendsNames := make([]string, 0)
	for _, cluster := range clusters {
		for _, backend := range cluster.Backends() {
			backendsNames = append(backendsNames, backend.Name)
		}
	}
	log.Debugf("Backend names %v\n", backendsNames)
	sCluster, err := newShard(name, backendsNames, st.Backends, st.syncLog)
	if err != nil {
		log.Fatalf("Initialization of region cluster %s failed reason: %s", name, err)
	}
	st.ShardClients[name] = sCluster
	return sCluster
}

// InitStorages setups storages
func InitStorages(transport http.RoundTripper, clustersConf config.ShardsMap,
	storagesMap config.StoragesMap, syncLog *SyncSender) (*Storages, error) {
	shards := make(map[string]NamedShardClient)
	storageClients := make(map[string]*StorageClient)

	if len(storagesMap) == 0 {
		return nil, fmt.Errorf("empty map 'storagesMap' in 'InitStorages'")
	}

	for name, storage := range storagesMap {
		if storage.Maintenance {
			log.Printf("storage %q in maintenance mode", name)
		}
		decoratedBackend, err := decorateBackend(transport, name, storage)
		if err != nil {
			return nil, err
		}
		storageClients[name] = decoratedBackend
	}

	if len(clustersConf) == 0 {
		return nil, fmt.Errorf("empty map 'clustersConf' in 'InitStorages'")
	}

	for name, clusterConf := range clustersConf {
		cluster, err := newShard(name, storageNames(clusterConf), storageClients, syncLog)
		cluster.balancer = balancing.NewBalancerPrioritySet(clusterConf.Storages, convertToRoundTrippersMap(storageClients))
		if err != nil {
			return nil, err
		}
		shards[name] = cluster
	}

	return &Storages{
		clustersConf: clustersConf,
		storagesMap:  storagesMap,
		syncLog:      syncLog,
		ShardClients: shards,
		Backends:     storageClients,
	}, nil
}

func convertToRoundTrippersMap(backends map[string]*StorageClient) map[string]http.RoundTripper {
	newMap := map[string]http.RoundTripper{}
	for key, backend := range backends {
		newMap[key] = backend
	}
	return newMap
}

func storageNames(conf config.Shard) []string {
	names := make([]string, 0)
	for _, storageConfig := range conf.Storages {
		names = append(names, storageConfig.Name)
	}
	return names
}

func decorateBackend(transport http.RoundTripper, name string, storageDef config.Storage) (*StorageClient, error) {

	errPrefix := fmt.Sprintf("initialization of backend '%s' resulted with error", name)
	decoratorFactory, ok := auth.Decorators[storageDef.Type]
	if !ok {
		return nil, fmt.Errorf("%s: no decorator defined for type '%s'", errPrefix, storageDef.Type)
	}
	decorator, err := decoratorFactory(name, storageDef)
	if err != nil {
		return nil, fmt.Errorf("%s: %q", errPrefix, err)
	}

	backend := &StorageClient{
		RoundTripper: httphandler.Decorate(transport, decorator, merger.ListV2Interceptor),
		Endpoint:     *storageDef.Backend.URL,
		Name:         name,
		Maintenance:  storageDef.Maintenance,
	}
	return backend, nil
}
