package sharding

import (
	"net/http"
	"net/url"

	"math"

	"github.com/allegro/akubra/config"
	"github.com/allegro/akubra/httphandler"
	shardingconfig "github.com/allegro/akubra/sharding/config"
	"github.com/allegro/akubra/storages"
	"github.com/allegro/akubra/transport"
	"github.com/serialx/hashring"
)

// RingFactory produces clients ShardsRing
type RingFactory struct {
	conf      config.Config
	transport http.RoundTripper
	storages  *storages.Storages
}

func (rf RingFactory) uniqBackends(regionCfg shardingconfig.RegionConfig) ([]url.URL, error) {
	allBackendsSet := make(map[shardingconfig.YAMLUrl]struct{})
	for _, clusterConfig := range regionCfg.Clusters {
		clientCluster, err := rf.storages.GetCluster(clusterConfig.Cluster)
		if err != nil {
			return nil, err
		}
		for _, backendURL := range clientCluster.Backends {
			allBackendsSet[backendURL] = struct{}{}
		}
	}
	var uniqBackendsSlice []url.URL
	for url := range allBackendsSet {
		uniqBackendsSlice = append(uniqBackendsSlice, *url.URL)
	}
	return uniqBackendsSlice, nil
}

func (rf RingFactory) getRegionClusters(regionCfg shardingconfig.RegionConfig) map[string]int {
	res := make(map[string]int, len(regionCfg.Clusters))
	for _, clusterConfig := range regionCfg.Clusters {
		res[clusterConfig.Cluster] = int(math.Floor(clusterConfig.Weight * 100))
	}
	return res
}

func (rf RingFactory) makeClusterMap(clientClusters map[string]int) (map[string]storages.Cluster, error) {
	res := make(map[string]storages.Cluster, len(clientClusters))
	for name := range clientClusters {
		cl, err := rf.storages.GetCluster(name)
		if err != nil {
			return nil, err
		}
		res[name] = cl
	}
	return res, nil
}

func (rf RingFactory) createRegressionMap(regionConfig shardingconfig.RegionConfig) (map[string]storages.Cluster, error) {
	regressionMap := make(map[string]storages.Cluster, len(regionConfig.Clusters))
	var previousCluster storages.Cluster
	for i, cluster := range regionConfig.Clusters {
		clientCluster, err := rf.storages.GetCluster(cluster.Cluster)
		if err != nil {
			return nil, err
		}
		if i > 0 {
			regressionMap[cluster.Cluster] = previousCluster
		}
		previousCluster = clientCluster
	}
	return regressionMap, nil
}

// RegionRing returns ShardsRing for region
func (rf RingFactory) RegionRing(regionCfg shardingconfig.RegionConfig) (ShardsRing, error) {
	clientClusters := rf.getRegionClusters(regionCfg)
	shardClusterMap, err := rf.makeClusterMap(clientClusters)
	if err != nil {
		return ShardsRing{}, err
	}
	cHashMap := hashring.NewWithWeights(clientClusters)
	allBackendsSlice, err := rf.uniqBackends(regionCfg)
	if err != nil {
		return ShardsRing{}, err
	}

	respHandler := httphandler.LateResponseHandler(rf.conf)

	regressionMap, err := rf.createRegressionMap(regionCfg)
	if err != nil {
		return ShardsRing{}, nil
	}
	allBackendsRoundTripper := transport.NewMultiTransport(
		rf.transport,
		allBackendsSlice,
		respHandler,
		rf.conf.MaintainedBackends)
	return ShardsRing{
		cHashMap,
		shardClusterMap,
		allBackendsRoundTripper,
		regressionMap,
		rf.conf.ClusterSyncLog}, nil
}

//NewRingFactory creates ring factory
func NewRingFactory(conf config.Config, storages *storages.Storages, transport http.RoundTripper) RingFactory {
	return RingFactory{
		conf:      conf,
		storages:  storages,
		transport: transport,
	}
}
