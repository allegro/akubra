package sharding

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
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

type shardsRing struct {
	ring                    *consistenthash.Map
	shardClusterMap         map[string]cluster
	allClustersRoundTripper http.RoundTripper
	clusterRegressionMap    map[string]cluster
	inconsistencyLog        log.Logger
}

func (sr shardsRing) isBucketPath(path string) bool {
	trimmedPath := strings.Trim(path, "/")
	return len(strings.Split(trimmedPath, "/")) == 1
}

func (sr shardsRing) Pick(key string) (cluster, error) {
	var shardName string

	shardName = sr.ring.Get(key)
	shardCluster, ok := sr.shardClusterMap[shardName]
	if !ok {
		return cluster{}, fmt.Errorf("no cluster for shard %s, cannot handle key %s", shardName, key)
	}

	return shardCluster, nil
}

type reqBody struct {
	r *bytes.Reader
}

func (rb *reqBody) rewind() error {
	_, err := rb.r.Seek(0, io.SeekStart)
	return err
}

func (rb *reqBody) Read(b []byte) (int, error) {
	return rb.r.Read(b)
}

func (rb *reqBody) Close() error {
	return nil
}

func copyRequest(origReq *http.Request) (*http.Request, error) {
	newReq := new(http.Request)
	*newReq = *origReq
	newReq.URL = &url.URL{}
	*newReq.URL = *origReq.URL
	newReq.Header = http.Header{}
	for k, v := range origReq.Header {
		for _, vv := range v {
			newReq.Header.Add(k, vv)
		}
	}
	if origReq.Body != nil {
		buf := new(bytes.Buffer)
		_, err := io.Copy(buf, origReq.Body)
		if err != nil {
			return nil, err
		}
		newReq.Body = &reqBody{bytes.NewReader(buf.Bytes())}
	}
	return newReq, nil
}

func (sr shardsRing) send(roundTripper http.RoundTripper, req *http.Request) (*http.Response, error) {
	// Rewind request body
	bodySeeker, ok := req.Body.(*reqBody)
	if ok {
		err := bodySeeker.rewind()
		if err != nil {
			return nil, err
		}
	}
	return roundTripper.RoundTrip(req)
}

func (sr shardsRing) regressionCall(cl cluster, req *http.Request) (string, *http.Response, error) {
	resp, err := sr.send(cl, req)
	// Do regression call if response status is > 400
	if (err != nil || resp.StatusCode > 400) && req.Method != http.MethodPut {
		rcl, ok := sr.clusterRegressionMap[cl.name]
		if ok {
			return sr.regressionCall(rcl, req)
		}
	}
	return cl.name, resp, err
}
func (sr *shardsRing) logInconsistency(key, expectedClusterName, actualClusterName string) {
	logJSON, err := json.Marshal(
		struct {
			Key      string
			Expected string
			Actual   string
		}{key, expectedClusterName, actualClusterName})
	if err == nil {
		sr.inconsistencyLog.Printf(fmt.Sprintf("%s", logJSON))
	}
}

func (sr shardsRing) RoundTrip(req *http.Request) (*http.Response, error) {
	reqCopy, err := copyRequest(req)
	if err != nil {
		return nil, err
	}

	if reqCopy.Method == http.MethodDelete || sr.isBucketPath(reqCopy.URL.Path) {
		return sr.allClustersRoundTripper.RoundTrip(reqCopy)
	}

	cl, err := sr.Pick(reqCopy.URL.Path)
	if err != nil {
		return nil, err
	}

	clusterName, resp, err := sr.regressionCall(cl, reqCopy)
	if clusterName != cl.name {
		sr.logInconsistency(reqCopy.URL.Path, cl.name, clusterName)
	}

	return resp, err
}

func newMultiBackendCluster(transp http.RoundTripper,
	multiResponseHandler transport.MultipleResponsesHandler,
	clusterConf config.ClusterConfig, name string) cluster {
	backends := make([]url.URL, len(clusterConf.Backends))

	for i, backend := range clusterConf.Backends {
		backends[i] = *backend.URL
	}

	multiTransport := transport.NewMultiTransport(
		transp,
		backends,
		multiResponseHandler)

	return cluster{
		multiTransport,
		clusterConf.Weight,
		clusterConf.Backends,
		name,
	}
}

type ringFactory struct {
	conf                    config.Config
	transport               http.RoundTripper
	multipleResponseHandler transport.MultipleResponsesHandler
	clusters                map[string]cluster
}

func (rf ringFactory) initCluster(name string) (cluster, error) {
	clusterConf, ok := rf.conf.Clusters[name]
	if !ok {
		return cluster{}, fmt.Errorf("no cluster %q in configuration", name)
	}
	return newMultiBackendCluster(rf.transport, rf.multipleResponseHandler, clusterConf, name), nil
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
	for _, name := range clientCfg.Clusters {
		clientCluster, err := rf.getCluster(name)
		if err != nil {
			return nil, err
		}
		for _, backendURL := range clientCluster.backends {
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
	allBackendsRoundTripper := transport.NewMultiTransport(
		rf.transport,
		allBackendsSlice,
		rf.multipleResponseHandler)
	regressionMap, err := rf.createRegressionMap(clientCfg.Clusters)
	if err != nil {
		return shardsRing{}, nil
	}
	return shardsRing{cHashMap, shardMap, allBackendsRoundTripper, regressionMap, rf.conf.ClusterSyncLog}, nil
}

func newRingFactory(conf config.Config, transport http.RoundTripper, respHandler transport.MultipleResponsesHandler) ringFactory {
	return ringFactory{
		conf:                    conf,
		transport:               transport,
		multipleResponseHandler: respHandler,
		clusters:                make(map[string]cluster),
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
	respHandler := httphandler.NewMultipleResponseHandler(conf)
	rings := newRingFactory(conf, httptransp, respHandler)
	// TODO: Multiple clients
	ring, err := rings.clientRing(*conf.Client)
	if err != nil {
		return nil, err
	}

	conf.Mainlog.Printf("Ring sharded into %d partitions", len(ring.shardClusterMap))

	roundTripper := httphandler.DecorateRoundTripper(conf, ring)
	return httphandler.NewHandlerWithRoundTripper(conf, roundTripper)
}
