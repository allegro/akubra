package sharding

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"sync/atomic"

	"github.com/allegro/akubra/config"
	"github.com/allegro/akubra/httphandler"
	"github.com/allegro/akubra/log"
	shardingconfig "github.com/allegro/akubra/sharding/config"
	"github.com/allegro/akubra/storages"
	set "github.com/deckarep/golang-set"
	"github.com/stretchr/testify/assert"
)

func makePrimaryConfiguration() config.Config {
	methodsSlice := []string{"PUT", "GET", "DELETE"}

	methodsSet := set.NewThreadUnsafeSet()
	for _, method := range methodsSlice {
		methodsSet.Add(method)
	}

	syncLogger := log.DefaultLogger
	accessLogger := log.DefaultLogger
	mainLogger := log.DefaultLogger
	clsyncLogger := log.DefaultLogger

	return config.Config{
		YamlConfig:        config.YamlConfig{},
		SyncLogMethodsSet: methodsSet,
		Synclog:           syncLogger,
		Accesslog:         accessLogger,
		Mainlog:           mainLogger,
		ClusterSyncLog:    clsyncLogger,
	}
}

func makeRegionRing(clusterWeights []float64, t *testing.T, handlerfunc func(w http.ResponseWriter, r *http.Request)) ShardsRing {
	config := makePrimaryConfiguration()
	clusterMap := make(map[string]shardingconfig.ClusterConfig)
	regionClusterList := make([]shardingconfig.MultiClusterConfig, 0, len(clusterWeights))
	for l := 0; l < len(clusterWeights); l++ {
		clusterName := fmt.Sprintf("cluster%d", l)

		//"Clusters" part...
		handlerfun := http.HandlerFunc(handlerfunc)
		ts := httptest.NewServer(handlerfun)
		backendURL, err := url.Parse(ts.URL)
		if err != nil {
			t.Error(err)
		}
		backendYamlURL := &shardingconfig.YAMLUrl{URL: backendURL}
		backends := []shardingconfig.YAMLUrl{*backendYamlURL}
		clusterConfig := shardingconfig.ClusterConfig{
			Backends: backends,
		}
		clusterMap[clusterName] = clusterConfig

		//"Regions" part...
		multiClusterConfig := &shardingconfig.MultiClusterConfig{
			Cluster: clusterName,
			Weight:  clusterWeights[l],
		}
		regionClusterList = append(regionClusterList, *multiClusterConfig)
	}
	domains := []string{"http://regiondomain.pl"}
	regionConfig := &shardingconfig.RegionConfig{
		Clusters: regionClusterList,
		Domains:  domains,
	}
	config.Clusters = clusterMap

	httptransp, err := httphandler.ConfigureHTTPTransport(config)
	if err != nil {
		t.Error(err)
	}
	ringStorages := &storages.Storages{
		Conf:      config,
		Transport: httptransp,
		Clusters:  make(map[string]storages.Cluster),
	}
	ringFactory := NewRingFactory(config, ringStorages, httptransp)
	regionRing, err := ringFactory.RegionRing(*regionConfig)
	if err != nil {
		t.Error(err)
	}
	return regionRing
}

func TestGetWithOneCluster(t *testing.T) {
	callCount := int32(0)
	f := func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		atomic.AddInt32(&callCount, 1)
	}
	regionRing := makeRegionRing([]float64{1}, t, f)
	reqURL, _ := url.Parse("http://allegro.pl/b/o")
	request := &http.Request{
		URL:    reqURL,
		Method: "GET",
	}
	response, _ := regionRing.DoRequest(request)
	assert.Equal(t, int32(1), callCount)
	assert.Equal(t, http.StatusOK, response.StatusCode)
}

func TestGetWithTwoClusters(t *testing.T) {
	callCount := int32(0)
	f := func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		atomic.AddInt32(&callCount, 1)
	}
	regionRing := makeRegionRing([]float64{1, 1}, t, f)
	reqURL, _ := url.Parse("http://allegro.pl/b/o")
	request := &http.Request{
		URL:    reqURL,
		Method: "GET",
	}
	response, _ := regionRing.DoRequest(request)
	assert.Equal(t, int32(1), callCount)
	assert.Equal(t, http.StatusOK, response.StatusCode)
}

func TestGetWithTwoClustersAndRegression(t *testing.T) {
	callCount := int32(0)
	f := func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		atomic.AddInt32(&callCount, 1)
	}
	regionRing := makeRegionRing([]float64{0, 1}, t, f)
	reqURL, _ := url.Parse("http://allegro.pl/b/o")
	request := &http.Request{
		URL:    reqURL,
		Method: "GET",
	}
	response, _ := regionRing.DoRequest(request)
	assert.Equal(t, int32(2), callCount)
	assert.Equal(t, http.StatusNotFound, response.StatusCode)
}

func TestDeleteWithTwoClusters(t *testing.T) {
	callCount := int32(0)
	f := func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		atomic.AddInt32(&callCount, 1)
	}
	regionRing := makeRegionRing([]float64{1, 1}, t, f)
	reqURL, _ := url.Parse("http://allegro.pl/b/o")
	request := &http.Request{
		URL:    reqURL,
		Method: "DELETE",
	}
	response, _ := regionRing.DoRequest(request)
	assert.Equal(t, int32(2), callCount)
	assert.Equal(t, http.StatusNotFound, response.StatusCode)
}

func TestPutWithTwoClustersAndBucketOnly(t *testing.T) {
	callCount := int32(0)
	f := func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		atomic.AddInt32(&callCount, 1)
	}
	regionRing := makeRegionRing([]float64{1, 1}, t, f)
	reqURL, _ := url.Parse("http://allegro.pl/b")
	request := &http.Request{
		URL:    reqURL,
		Method: "PUT",
	}
	response, _ := regionRing.DoRequest(request)
	assert.Equal(t, int32(2), callCount)
	assert.Equal(t, http.StatusOK, response.StatusCode)
}
