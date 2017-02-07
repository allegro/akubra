package sharding

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/allegro/akubra/config"
	"github.com/allegro/akubra/httphandler"
	"github.com/allegro/akubra/log"
	"github.com/allegro/akubra/transport"
	"github.com/golang/groupcache/consistenthash"
)

type cluster struct {
	http.RoundTripper
	weight   uint64
	backends []config.YAMLURL
	name     string
}

func newMultiBackendCluster(transp http.RoundTripper,
	multiResponseHandler transport.MultipleResponsesHandler,
	clusterConf config.ClusterConfig, name string, maintainedBackends []config.YAMLURL) cluster {
	backends := make([]url.URL, len(clusterConf.Backends))

	for i, backend := range clusterConf.Backends {
		backends[i] = *backend.URL
	}

	multiTransport := transport.NewMultiTransport(
		transp,
		backends,
		multiResponseHandler,
		maintainedBackends)

	return cluster{
		multiTransport,
		clusterConf.Weight,
		clusterConf.Backends,
		name,
	}
}

type ringFactory struct {
	conf      config.Config
	transport http.RoundTripper
	clusters  map[string]cluster
}

func (rf ringFactory) initCluster(name string) (cluster, error) {
	clusterConf, ok := rf.conf.Clusters[name]
	if !ok {
		return cluster{}, fmt.Errorf("no cluster %q in configuration", name)
	}
	respHandler := httphandler.EarliestResponseHandler(rf.conf)
	return newMultiBackendCluster(rf.transport, respHandler, clusterConf, name, rf.conf.MaintainedBackends), nil
}

func (rf ringFactory) getCluster(name string) (cluster, error) {
	s3cluster, ok := rf.clusters[name]
	if ok {
		return s3cluster, nil
	}
	s3cluster, err := rf.initCluster(name)
	if err != nil {
		return s3cluster, err
	}
	rf.clusters[name] = s3cluster
	return s3cluster, nil
}

func (rf ringFactory) mapShards(weightSum uint64, clientCfg config.ClientConfig) (map[string]cluster, error) {
	shardClusterMap := make(map[string]cluster, clientCfg.ShardsCount)
	offset := 0
	for _, name := range clientCfg.Clusters {
		clientCluster, err := rf.getCluster(name)
		if err != nil {
			return shardClusterMap, err
		}
		// shardsNum := float64(clientCfg.ShardsCount * clientCluster.weight) / float64(weightSum)
		shardsNum := (clientCfg.ShardsCount * clientCluster.weight) / weightSum
		for i := offset; i < offset+int(shardsNum); i++ {
			shardName := fmt.Sprintf("%s-%d", clientCfg.Name, i)
			shardClusterMap[shardName] = clientCluster
		}
		offset += int(shardsNum)
	}
	return shardClusterMap, nil
}

func (rf ringFactory) uniqBackends(clientCfg config.ClientConfig) ([]url.URL, error) {
	allBackendsSet := make(map[config.YAMLURL]bool)
	log.Debugf("client %v", clientCfg.Clusters)
	for _, name := range clientCfg.Clusters {
		log.Debugf("cluster %s", name)
		clientCluster, err := rf.getCluster(name)
		if err != nil {
			return nil, err
		}
		for _, backendURL := range clientCluster.backends {
			log.Debugf("backend %s", backendURL.Host)
			allBackendsSet[backendURL] = true
		}
	}
	var uniqBackendsSlice []url.URL
	for url := range allBackendsSet {
		uniqBackendsSlice = append(uniqBackendsSlice, *url.URL)
	}
	return uniqBackendsSlice, nil
}

func (rf ringFactory) sumWeights(clusters []string) (uint64, error) {
	weightSum := uint64(0)
	for _, name := range clusters {
		clientCluster, err := rf.getCluster(name)
		if err != nil {
			return 0, err
		}
		weightSum += clientCluster.weight
	}
	return weightSum, nil
}

func (rf ringFactory) createRegressionMap(clusters []string) (map[string]cluster, error) {
	regressionMap := make(map[string]cluster)
	var previousCluster cluster
	for i, name := range clusters {
		clientCluster, err := rf.getCluster(name)
		if err != nil {
			return nil, err
		}
		if i > 0 {
			regressionMap[name] = previousCluster
		}
		previousCluster = clientCluster
	}
	return regressionMap, nil
}

func (rf ringFactory) clientRing(clientCfg config.ClientConfig) (shardsRing, error) {
	weightSum, err := rf.sumWeights(clientCfg.Clusters)

	if err != nil {
		return shardsRing{}, err
	}

	if weightSum <= 0 {
		return shardsRing{}, fmt.Errorf("configuration error clusters weigth sum should be greater than 0, got %d", weightSum)
	}

	shardMap, err := rf.mapShards(weightSum, clientCfg)
	if err != nil {
		return shardsRing{}, err
	}

	cHashMap := consistenthash.New(1, nil)
	for shardID := range shardMap {
		cHashMap.Add(shardID)
	}

	allBackendsSlice, err := rf.uniqBackends(clientCfg)
	if err != nil {
		return shardsRing{}, err
	}

	respHandler := httphandler.LateResponseHandler(rf.conf)

	allBackendsRoundTripper := transport.NewMultiTransport(
		rf.transport,
		allBackendsSlice,
		respHandler,
		rf.conf.MaintainedBackends)
	log.Debugf("All backends %v", allBackendsSlice)
	regressionMap, err := rf.createRegressionMap(clientCfg.Clusters)
	if err != nil {
		return shardsRing{}, nil
	}
	return shardsRing{
		cHashMap,
		shardMap,
		allBackendsRoundTripper,
		regressionMap,
		rf.conf.ClusterSyncLog}, nil
}

func newRingFactory(conf config.Config, transport http.RoundTripper) ringFactory {
	return ringFactory{
		conf:      conf,
		transport: transport,
		clusters:  make(map[string]cluster),
	}
}

// NewHandler constructs http.Handler
func NewHandler(conf config.Config) (http.Handler, error) {
	clustersNames := make([]string, 0, len(conf.Clusters))
	for name := range conf.Clusters {
		clustersNames = append(clustersNames, name)
	}

	conf.Mainlog.Printf("Configured clusters: %s", strings.Join(clustersNames, ", "))

	httptransp, err := httphandler.ConfigureHTTPTransport(conf)
	if err != nil {
		return nil, err
	}

	rings := newRingFactory(conf, httptransp)
	// TODO: Multiple clients
	ring, err := rings.clientRing(*conf.Client)
	if err != nil {
		return nil, err
	}

	conf.Mainlog.Printf("Ring sharded into %d partitions", len(ring.shardClusterMap))

	roundTripper := httphandler.DecorateRoundTripper(conf, ring)
	return httphandler.NewHandlerWithRoundTripper(conf, roundTripper)
}
