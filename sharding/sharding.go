package sharding

import (
	"fmt"
	"net/http"

	"math"

	"github.com/allegro/akubra/log"
	"github.com/allegro/akubra/regions/config"
	"github.com/allegro/akubra/storages"
	"github.com/serialx/hashring"
)

// RingFactory produces clients ShardsRing
type RingFactory struct {
	conf      config.Regions
	transport http.RoundTripper
	storages  storages.Storages
	syncLog   log.Logger
}

func (rf RingFactory) createRegressionMap(config config.Region) (map[string]storages.NamedCluster, error) {
	regressionMap := make(map[string]storages.NamedCluster)
	var previousCluster storages.NamedCluster
	for i, cluster := range config.Clusters {
		clientCluster, err := rf.storages.GetCluster(cluster.Name)
		if err != nil {
			return nil, err
		}
		if i > 0 {
			regressionMap[cluster.Name] = previousCluster
		}
		previousCluster = clientCluster
	}
	return regressionMap, nil
}

func (rf RingFactory) getRegionClustersWeights(regionCfg config.Region) map[string]int {
	res := make(map[string]int)
	for _, clusterConfig := range regionCfg.Clusters {
		res[clusterConfig.Name] = int(math.Floor(clusterConfig.Weight * 100))
	}
	return res
}

func (rf RingFactory) makeRegionClusterMap(clientClusters map[string]int) (map[string]storages.NamedCluster, error) {
	res := make(map[string]storages.NamedCluster, len(clientClusters))
	for name := range clientClusters {
		cl, err := rf.storages.GetCluster(name)
		if err != nil {
			return nil, err
		}
		res[name] = cl
	}
	return res, nil
}

// RegionRing returns ShardsRing for region
func (rf RingFactory) RegionRing(name string, regionCfg config.Region) (ShardsRing, error) {
	clustersWeights := rf.getRegionClustersWeights(regionCfg)

	shardClusterMap, err := rf.makeRegionClusterMap(clustersWeights)
	if err != nil {
		return ShardsRing{}, err
	}
	var regionClusters []storages.NamedCluster
	for _, cluster := range shardClusterMap {
		regionClusters = append(regionClusters, cluster)
	}

	cHashMap := hashring.NewWithWeights(clustersWeights)

	allBackendsRoundTripper := rf.storages.JoinClusters(fmt.Sprintf("region-%s", name), regionClusters...)
	regressionMap, err := rf.createRegressionMap(regionCfg)
	if err != nil {
		return ShardsRing{}, err
	}

	// respHandler := httphandler.LateResponseHandler(rf.conf)

	return ShardsRing{
		cHashMap,
		shardClusterMap,
		allBackendsRoundTripper,
		regressionMap,
		rf.syncLog}, nil
}

//NewRingFactory creates ring factory
func NewRingFactory(conf config.Regions, storages storages.Storages, transport http.RoundTripper, syncLog log.Logger) RingFactory {
	return RingFactory{
		conf:      conf,
		storages:  storages,
		transport: transport,
		syncLog:   syncLog,
	}
}
