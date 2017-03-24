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
	units "github.com/docker/go-units"
	"github.com/serialx/hashring"
)

type cluster struct {
	http.RoundTripper
	weight   int
	backends []config.YAMLUrl
	name     string
}

func newMultiBackendCluster(transp http.RoundTripper,
	multiResponseHandler transport.MultipleResponsesHandler,
	clusterConf config.ClusterConfig, name string, maintainedBackends []config.YAMLUrl) cluster {
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

func (rf ringFactory) uniqBackends(clientCfg config.ClientConfig) ([]url.URL, error) {
	allBackendsSet := make(map[config.YAMLUrl]bool)
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

func (rf ringFactory) getClientClusters(clientCfg config.ClientConfig) map[string]int {
	res := make(map[string]int)
	for _, clusterName := range clientCfg.Clusters {
		cluster := rf.conf.Clusters[clusterName]
		res[clusterName] = cluster.Weight
	}
	return res
}

func (rf ringFactory) makeClusterMap(clientClusters map[string]int) (map[string]cluster, error) {
	res := make(map[string]cluster, len(clientClusters))
	for name := range clientClusters {
		cl, err := rf.getCluster(name)
		if err != nil {
			return nil, err
		}
		res[name] = cl
	}
	return res, nil
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
	clientClusters := rf.getClientClusters(clientCfg)

	shardClusterMap, err := rf.makeClusterMap(clientClusters)
	if err != nil {
		return shardsRing{}, err
	}

	cHashMap := hashring.NewWithWeights(clientClusters)
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
		shardClusterMap,
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
	bodyMaxSize, err := units.FromHumanSize(conf.BodyMaxSize)
	if err != nil {
		return nil, fmt.Errorf("Unable to parse BodyMaxSize: %s" + err.Error())
	}

	return httphandler.NewHandlerWithRoundTripper(roundTripper, bodyMaxSize)
}
