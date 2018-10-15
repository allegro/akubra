package sharding

import (
	"fmt"
	"math"

	"github.com/allegro/akubra/log"
	"github.com/allegro/akubra/regions/config"
	"github.com/allegro/akubra/storages"
	"github.com/serialx/hashring"
)

// RingFactory produces clients ShardsRing
type RingFactory struct {
	conf     config.ShardingPolicies
	storages storages.ClusterStorage
	syncLog  log.Logger
}

func (rf RingFactory) createRegressionMap(config config.Policies) (map[string]storages.NamedShardClient, error) {
	regressionMap := make(map[string]storages.NamedShardClient)
	lastClusterName := config.Shards[len(config.Shards)-1].ShardName
	previousCluster, err := rf.storages.GetShard(lastClusterName)
	if err != nil {
		log.Printf("Last cluster in region not defined in storages")
	}
	for _, cluster := range config.Shards {
		clientCluster, err := rf.storages.GetShard(cluster.ShardName)
		if err != nil {
			return nil, err
		}
		regressionMap[cluster.ShardName] = previousCluster
		previousCluster = clientCluster
	}
	return regressionMap, nil
}

func (rf RingFactory) getRegionClustersWeights(regionCfg config.Policies) map[string]int {
	res := make(map[string]int)
	for _, clusterConfig := range regionCfg.Shards {
		res[clusterConfig.ShardName] = int(math.Floor(clusterConfig.Weight * 100))
	}
	return res
}

func (rf RingFactory) makeRegionClusterMap(clientClusters map[string]int) (map[string]storages.NamedShardClient, error) {
	res := make(map[string]storages.NamedShardClient, len(clientClusters))
	for name := range clientClusters {
		cl, err := rf.storages.GetShard(name)
		if err != nil {
			return nil, err
		}
		res[name] = cl
	}
	return res, nil
}

// RegionRing returns ShardsRing for region
func (rf RingFactory) RegionRing(name string, regionCfg config.Policies) (ShardsRing, error) {
	clustersWeights := rf.getRegionClustersWeights(regionCfg)

	shardClusterMap, err := rf.makeRegionClusterMap(clustersWeights)
	if err != nil {
		log.Debugf("cluster map creation error %s\n", err)
		return ShardsRing{}, err
	}
	var regionShards []storages.NamedShardClient
	for _, cluster := range shardClusterMap {
		regionShards = append(regionShards, cluster)
	}

	cHashMap := hashring.NewWithWeights(clustersWeights)

	allBackendsRoundTripper := rf.storages.MergeShards(fmt.Sprintf("region-%s", name), regionShards...)
	regressionMap, err := rf.createRegressionMap(regionCfg)
	if err != nil {
		return ShardsRing{}, err
	}

	return ShardsRing{
		ring:                    cHashMap,
		shardClusterMap:         shardClusterMap,
		allClustersRoundTripper: allBackendsRoundTripper,
		clusterRegressionMap:    regressionMap,
		inconsistencyLog:        rf.syncLog}, nil
}

// NewRingFactory creates ring factory
func NewRingFactory(conf config.ShardingPolicies, storages storages.ClusterStorage, syncLog log.Logger) RingFactory {
	return RingFactory{
		conf:     conf,
		storages: storages,
		syncLog:  syncLog,
	}
}
