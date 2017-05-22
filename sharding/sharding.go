package sharding

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/allegro/akubra/config"
	"github.com/allegro/akubra/httphandler"
	"github.com/allegro/akubra/log"
	shardingconfig "github.com/allegro/akubra/sharding/config"
	"github.com/allegro/akubra/transport"
	"github.com/serialx/hashring"
)

// Cluster stores information about cluster backends
type Cluster struct {
	http.RoundTripper
	weight   int
	Backends []shardingconfig.YAMLUrl
	Name     string
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
		clusterConf.Weight,
		clusterConf.Backends,
		name,
	}
}

// RingFactory produces clients ShardsRing
type RingFactory struct {
	conf      config.Config
	transport http.RoundTripper
	clusters  map[string]Cluster
}

func (rf RingFactory) initCluster(name string) (Cluster, error) {
	clusterConf, ok := rf.conf.Clusters[name]
	if !ok {
		return Cluster{}, fmt.Errorf("no cluster %q in configuration", name)
	}
	respHandler := httphandler.EarliestResponseHandler(rf.conf)
	return newMultiBackendCluster(rf.transport, respHandler, clusterConf, name, rf.conf.MaintainedBackends), nil
}

func (rf RingFactory) getCluster(name string) (Cluster, error) {
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

func (rf RingFactory) uniqBackends(clientCfg shardingconfig.ClientConfig) ([]url.URL, error) {
	allBackendsSet := make(map[shardingconfig.YAMLUrl]bool)
	log.Debugf("client %v", clientCfg.Clusters)
	for _, name := range clientCfg.Clusters {
		log.Debugf("cluster %s", name)
		clientCluster, err := rf.getCluster(name)
		if err != nil {
			return nil, err
		}
		for _, backendURL := range clientCluster.Backends {
			allBackendsSet[backendURL] = true
		}
	}
	var uniqBackendsSlice []url.URL
	for url := range allBackendsSet {
		uniqBackendsSlice = append(uniqBackendsSlice, *url.URL)
	}
	return uniqBackendsSlice, nil
}

func (rf RingFactory) getClientClusters(clientCfg shardingconfig.ClientConfig) map[string]int {
	res := make(map[string]int)
	for _, clusterName := range clientCfg.Clusters {
		cluster := rf.conf.Clusters[clusterName]
		res[clusterName] = cluster.Weight
	}
	return res
}

func (rf RingFactory) makeClusterMap(clientClusters map[string]int) (map[string]Cluster, error) {
	res := make(map[string]Cluster, len(clientClusters))
	for name := range clientClusters {
		cl, err := rf.getCluster(name)
		if err != nil {
			return nil, err
		}
		res[name] = cl
	}
	return res, nil
}

func (rf RingFactory) createRegressionMap(clusters []string) (map[string]Cluster, error) {
	regressionMap := make(map[string]Cluster)
	var previousCluster Cluster
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

// ClientRing returns clients ShardsRing
func (rf RingFactory) ClientRing(clientCfg shardingconfig.ClientConfig) (ShardsRing, error) {
	clientClusters := rf.getClientClusters(clientCfg)

	shardClusterMap, err := rf.makeClusterMap(clientClusters)
	if err != nil {
		return ShardsRing{}, err
	}

	cHashMap := hashring.NewWithWeights(clientClusters)
	allBackendsSlice, err := rf.uniqBackends(clientCfg)
	if err != nil {
		return ShardsRing{}, err
	}

	respHandler := httphandler.LateResponseHandler(rf.conf)

	allBackendsRoundTripper := transport.NewMultiTransport(
		rf.transport,
		allBackendsSlice,
		respHandler,
		rf.conf.MaintainedBackends)
	regressionMap, err := rf.createRegressionMap(clientCfg.Clusters)
	if err != nil {
		return ShardsRing{}, nil
	}
	return ShardsRing{
		cHashMap,
		shardClusterMap,
		allBackendsRoundTripper,
		regressionMap,
		rf.conf.ClusterSyncLog}, nil
}

// NewRingFactory returns sharding ring factory
func NewRingFactory(conf config.Config, transport http.RoundTripper) RingFactory {
	return RingFactory{
		conf:      conf,
		transport: transport,
		clusters:  make(map[string]Cluster),
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

	rings := NewRingFactory(conf, httptransp)
	// TODO: Multiple clients
	ring, err := rings.ClientRing(*conf.Client)
	if err != nil {
		return nil, err
	}

	conf.Mainlog.Printf("Ring sharded into %d partitions", len(ring.shardClusterMap))

	roundTripper := httphandler.DecorateRoundTripper(conf, ring)

	return httphandler.NewHandlerWithRoundTripper(roundTripper, conf.BodyMaxSize.SizeInBytes, conf.MaxConcurrentRequests)
}
